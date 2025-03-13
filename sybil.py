import asyncio
import os
import socket
import logging
import aiosqlite
import datetime
import json
import time
import libtorrent as lt
from collections import deque
from hashlib import sha1
import bencode_open

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def create_tables(db):
    await db.execute('''
        CREATE TABLE IF NOT EXISTS magnet_links (
            infohash BLOB PRIMARY KEY,
            first_discovered TIMESTAMP,
            name TEXT,
            size INTEGER,
            file_count INTEGER,
            file_tree TEXT,
            last_updated TIMESTAMP
        )
    ''')
    await db.execute('''
        CREATE TABLE IF NOT EXISTS peers (
            infohash BLOB,
            ip TEXT,
            port INTEGER,
            discovered TIMESTAMP,
            PRIMARY KEY (infohash, ip, port)
        )
    ''')
    await db.commit()

class DHTCrawler(asyncio.DatagramProtocol):
    def __init__(self, port, db):
        self.port = port
        self.node_id = os.urandom(20)
        self.transactions = {}
        self.known_nodes = set()
        self.nodes_queue = deque()
        self.bootstrap_nodes = [
            ('router.bittorrent.com', 6881),
            ('dht.transmissionbt.com', 6881),
            ('router.utorrent.com', 6881)
        ]
        self.transport = None
        self.db = db
        self.metadata_semaphore = asyncio.Semaphore(10)  # Concurrent metadata fetches

    def connection_made(self, transport):
        self.transport = transport

    async def start(self):
        loop = asyncio.get_running_loop()
        await loop.create_datagram_endpoint(
            lambda: self,
            local_addr=('0.0.0.0', self.port)
        )
        for node in self.bootstrap_nodes:
            self._add_node(node)
        asyncio.create_task(self._process_queue())
        logging.info(f"DHT node started on port {self.port}")

    def _add_node(self, node):
        ip, port = node
        if (ip, port) not in self.known_nodes:
            self.known_nodes.add((ip, port))
            self.nodes_queue.append((ip, port))

    async def _process_queue(self):
        while True:
            if not self.nodes_queue:
                await asyncio.sleep(1)
                continue
            ip, port = self.nodes_queue.popleft()
            await self._send_find_node((ip, port))
            await asyncio.sleep(0.1)  # Rate limit

    async def _send_find_node(self, address):
        try:
            tid = os.urandom(2)
            query = {
                b't': tid,
                b'y': b'q',
                b'q': b'find_node',
                b'a': {
                    b'id': self.node_id,
                    b'target': os.urandom(20)
                }
            }
            self.transport.sendto(bencode_open.dumps(query), address)
            self.transactions[tid] = (address, 'find_node')
        except Exception as e:
            logging.error(f"Error sending find_node: {e}")

    def datagram_received(self, data, addr):
        try:
            msg = bencode_open.loads(data)
        except Exception as e:
            return

        if msg.get(b'y') == b'r':
            self._handle_response(msg, addr)
        elif msg.get(b'y') == b'q':
            self._handle_query(msg, addr)

    def _handle_query(self, msg, addr):
        query_type = msg.get(b'q')
        if query_type == b'announce_peer':
            self._handle_announce_peer(msg, addr)

    def _handle_announce_peer(self, msg, addr):
        args = msg.get(b'a', {})
        infohash = args.get(b'info_hash')
        if not infohash or len(infohash) != 20:
            return

        implied_port = args.get(b'implied_port', 0)
        port = addr[1] if implied_port else args.get(b'port', addr[1])
        asyncio.create_task(self._log_magnet(infohash, addr[0], port))

    async def _log_magnet(self, infohash, ip, port):
        try:
            now = datetime.datetime.utcnow().isoformat()
            async with self.db.execute(
                'INSERT OR IGNORE INTO magnet_links (infohash, first_discovered) VALUES (?, ?)',
                (infohash, now)
            ) as cursor:
                if cursor.rowcount == 1:
                    asyncio.create_task(self._fetch_metadata(infohash))
            
            await self.db.execute(
                'INSERT OR IGNORE INTO peers (infohash, ip, port, discovered) VALUES (?, ?, ?, ?)',
                (infohash, ip, port, now)
            )
            await self.db.commit()
        except Exception as e:
            logging.error(f"Database error: {e}")

    async def _fetch_metadata(self, infohash):
        async with self.metadata_semaphore:
            try:
                loop = asyncio.get_running_loop()
                hex_infohash = infohash.hex()
                magnet = f'magnet:?xt=urn:btih:{hex_infohash}'
                
                def sync_fetch():
                    ses = lt.session()
                    ses.listen_on(6881, 6891)
                    params = {'save_path': '/dev/null', 'storage_mode': lt.storage_mode_t.storage_mode_sparse}
                    handle = lt.add_magnet_uri(ses, magnet, params)
                    start = time.time()
                    while not handle.has_metadata() and time.time() - start < 30:
                        time.sleep(1)
                    if not handle.has_metadata():
                        raise TimeoutError()
                    return handle.get_torrent_info()
                
                torrent_info = await loop.run_in_executor(None, sync_fetch)
                name = torrent_info.name()
                size = torrent_info.total_size()
                files = [{'path': f.path, 'size': f.size} for f in torrent_info.files()]
                
                await self.db.execute(
                    '''UPDATE magnet_links SET name = ?, size = ?, file_count = ?, file_tree = ?, last_updated = ?
                    WHERE infohash = ?''',
                    (name, size, len(files), json.dumps(files), datetime.datetime.utcnow().isoformat(), infohash)
                )
                await self.db.commit()
                logging.info(f"Fetched metadata for {hex_infohash}")
            except Exception as e:
                logging.error(f"Metadata fetch failed: {e}")

    def _handle_response(self, msg, addr):
        tid = msg.get(b't')
        if not tid or tid not in self.transactions:
            return
        
        original_addr, query_type = self.transactions.pop(tid)
        response = msg.get(b'r', {})
        
        if query_type == 'find_node':
            nodes = response.get(b'nodes', b'')
            for i in range(0, len(nodes), 26):
                node = nodes[i:i+26]
                if len(node) < 26:
                    continue
                ip = socket.inet_ntoa(node[20:24])
                port = int.from_bytes(node[24:26], 'big')
                self._add_node((ip, port))

async def main():
    async with aiosqlite.connect('dht_data.db') as db:
        await create_tables(db)
        num_nodes = 200
        crawlers = [DHTCrawler(49000 + i, db) for i in range(1, num_nodes + 1)]
        await asyncio.gather(*(crawler.start() for crawler in crawlers))
        
        try:
            while True:
                await asyncio.sleep(3600)
        except KeyboardInterrupt:
            for crawler in crawlers:
                if crawler.transport:
                    crawler.transport.close()

if __name__ == "__main__":
    asyncio.run(main())
