import argparse
import asyncio
import json
import logging
import os
import sys

from raftkv.server import serve_node

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("RaftKVMain")

def parse_peers(peers_env: str) -> dict:
    if not peers_env:
        return {}
    if peers_env.startswith("{"):
        try:
            return json.loads(peers_env)
        except Exception:
            pass
            
    peers = {}
    tokens = [t.strip() for t in peers_env.split(",") if t.strip()]
    for token in tokens:
        if "=" in token:
            node_id, addr = token.split("=", 1)
            peers[node_id.strip()] = addr.strip()
        elif ":" in token:
            host = token.split(":")[0]
            peers[host] = token.strip()
    return peers

def main():
    parser = argparse.ArgumentParser(description="RaftKV Distributed Key-Value Store Node")
    parser.add_argument("--node-id", default=os.getenv("NODE_ID", "raft-node-1"), help="Unique Node ID")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "50051")), help="gRPC Listening Port")
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR", "./data"), help="Data storage directory")
    parser.add_argument("--peers", default=os.getenv("PEER_ADDRESSES", ""), help="Comma separated peers (e.g. node1=127.0.0.1:50051,node2=127.0.0.1:50052)")

    args = parser.parse_args()

    peer_addresses = parse_peers(args.peers)
    logger.info(f"Starting RaftKV Node: ID={args.node_id}, Port={args.port}, DataDir={args.data_dir}")
    logger.info(f"Cluster Peer Addresses: {peer_addresses}")

    try:
        asyncio.run(serve_node(
            node_id=args.node_id,
            peer_addresses=peer_addresses,
            port=args.port,
            data_dir=args.data_dir
        ))
    except KeyboardInterrupt:
        logger.info("RaftKV Node shutting down cleanly.")

if __name__ == "__main__":
    main()
