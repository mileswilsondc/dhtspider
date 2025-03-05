import asyncio
import os
import socket
import logging
from collections import deque
import bencode_open

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DHTCrawler(asyncio.DatagramProtocol):
    def __init__(self):
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

    def connection_made(self, transport):
        """Required protocol method"""
        self.transport = transport

    async def start(self):
        """Start the DHT crawler"""
        loop = asyncio.get_running_loop()
        await loop.create_datagram_endpoint(
            lambda: self,
            local_addr=('0.0.0.0', 59829)
        )
        for node in self.bootstrap_nodes:
            self._add_node(node)
        asyncio.create_task(self._process_queue())
        logging.info("DHT crawler started.")

    def _add_node(self, node):
        ip, port = node
        if (ip, port) not in self.known_nodes:
            self.known_nodes.add((ip, port))
            self.nodes_queue.append((ip, port))
            logging.debug(f"Added node {ip}:{port} to queue.")

    async def _process_queue(self):
        while True:
            if not self.nodes_queue:
                await asyncio.sleep(1)
                continue
            ip, port = self.nodes_queue.popleft()
            logging.debug(f"Querying node {ip}:{port}")
            await self._send_find_node((ip, port))
            await asyncio.sleep(0.1)

    async def _send_find_node(self, address):
        try:
            ip, port = address
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
            data = bencode_open.dumps(query)
            self.transport.sendto(data, (ip, port))
            self.transactions[tid] = (address, 'find_node')
            logging.debug(f"Sent find_node to {ip}:{port}")
        except Exception as e:
            logging.error(f"Error sending find_node to {address}: {e}")

    def datagram_received(self, data, addr):
        try:
            msg = bencode_open.loads(data)
        except Exception as e:
            logging.warning(f"Failed to decode message: {e}")
            return

        if msg.get(b'y') == b'r':
            tid = msg.get(b't')
            if tid is None or tid not in self.transactions:
                return
            original_addr, query_type = self.transactions.pop(tid)
            self._handle_response(msg[b'r'], original_addr, query_type)
        elif msg.get(b'y') == b'e':
            logging.warning(f"Error response: {msg.get(b'e')}")

    def _handle_response(self, response, address, query_type):
        if query_type == 'find_node':
            self._handle_find_node_response(response, address)

    def _handle_find_node_response(self, response, address):
        node_id = response.get(b'id')
        if not node_id or len(node_id) != 20:
            return

        nodes = response.get(b'nodes', b'')
        for i in range(0, len(nodes), 26):
            node_data = nodes[i:i+26]
            if len(node_data) < 26:
                break
            try:
                ip = socket.inet_ntoa(node_data[20:24])
                port = int.from_bytes(node_data[24:26], 'big')
                self._add_node((ip, port))
            except Exception as e:
                logging.warning(f"Error parsing node info: {e}")

        logging.info(f"Discovered {len(nodes)//26} nodes from {address[0]}:{address[1]}")

    def error_received(self, exc):
        logging.error(f"UDP error received: {exc}")

    def connection_lost(self, exc):
        logging.info("UDP connection closed")

async def main():
    crawler = DHTCrawler()
    await crawler.start()
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        crawler.transport.close()

if __name__ == "__main__":
    asyncio.run(main())
