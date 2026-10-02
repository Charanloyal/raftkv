import argparse
import asyncio
import logging
import sys
import uuid
import time
from typing import List, Optional, Tuple
import grpc

from raftkv.proto import raft_pb2, raft_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RaftKVClient")

class RaftKVClient:
    """
    Smart, Fault-Tolerant Client for RaftKV Distributed Store.
    Handles leader redirection, client request deduplication, and node failovers.
    """
    def __init__(self, cluster_addresses: List[str]):
        self.cluster_addresses = [a.strip() for a in cluster_addresses if a.strip()]
        if not self.cluster_addresses:
            raise ValueError("At least one cluster address must be provided.")
            
        self.client_id = f"client-{uuid.uuid4().hex[:8]}"
        self.sequence_num = 0
        self.leader_index = 0
        self.known_leader_addr: Optional[str] = None

    def _next_seq(self) -> int:
        self.sequence_num += 1
        return self.sequence_num

    def _get_target_address(self) -> str:
        if self.known_leader_addr:
            return self.known_leader_addr
        return self.cluster_addresses[self.leader_index % len(self.cluster_addresses)]

    async def put(self, key: str, value: str, timeout: float = 10.0) -> bool:
        seq = self._next_seq()
        start_time = time.monotonic()
        attempts = 0

        while time.monotonic() - start_time < timeout:
            attempts += 1
            target_addr = self._get_target_address()
            logger.debug(f"[Client] Sending PUT(key='{key}', val='{value}') to {target_addr} (seq={seq})")

            try:
                async with grpc.aio.insecure_channel(target_addr) as channel:
                    stub = raft_pb2_grpc.ClientServiceStub(channel)
                    req = raft_pb2.PutArgs(
                        key=key,
                        value=value,
                        client_id=self.client_id,
                        sequence_num=seq
                    )
                    reply: raft_pb2.PutReply = await stub.Put(req, timeout=2.0)

                    if reply.success:
                        logger.info(f"PUT key='{key}' succeeded on {target_addr}.")
                        return True

                    if reply.error == "NOT_LEADER":
                        if reply.leader_address:
                            logger.info(f"Leader redirection hint received: {target_addr} -> {reply.leader_address}")
                            self.known_leader_addr = reply.leader_address
                        else:
                            logger.info(f"Node {target_addr} is not leader, no hint. Rotating target node.")
                            self.known_leader_addr = None
                            self.leader_index += 1
                    else:
                        logger.warning(f"PUT error from {target_addr}: {reply.error}. Retrying...")
                        await asyncio.sleep(0.2)

            except Exception as e:
                logger.warning(f"Connection failed to {target_addr}: {e}. Rotating node.")
                self.known_leader_addr = None
                self.leader_index += 1
                await asyncio.sleep(0.3)

        logger.error(f"PUT key='{key}' failed after {attempts} attempts and timeout.")
        return False

    async def get(self, key: str, timeout: float = 10.0) -> Tuple[bool, str]:
        seq = self._next_seq()
        start_time = time.monotonic()

        while time.monotonic() - start_time < timeout:
            target_addr = self._get_target_address()
            try:
                async with grpc.aio.insecure_channel(target_addr) as channel:
                    stub = raft_pb2_grpc.ClientServiceStub(channel)
                    req = raft_pb2.GetArgs(
                        key=key,
                        client_id=self.client_id,
                        sequence_num=seq
                    )
                    reply: raft_pb2.GetReply = await stub.Get(req, timeout=2.0)

                    if reply.success:
                        if reply.found:
                            logger.info(f"GET key='{key}' -> value='{reply.value}' from {target_addr}")
                            return True, reply.value
                        else:
                            logger.info(f"GET key='{key}' -> KEY NOT FOUND from {target_addr}")
                            return False, ""

                    if reply.error == "NOT_LEADER":
                        if reply.leader_address:
                            self.known_leader_addr = reply.leader_address
                        else:
                            self.known_leader_addr = None
                            self.leader_index += 1
                    else:
                        await asyncio.sleep(0.2)

            except Exception as e:
                logger.warning(f"GET connection failed to {target_addr}: {e}. Rotating node.")
                self.known_leader_addr = None
                self.leader_index += 1
                await asyncio.sleep(0.3)

        return False, ""

    async def delete(self, key: str, timeout: float = 10.0) -> bool:
        seq = self._next_seq()
        start_time = time.monotonic()

        while time.monotonic() - start_time < timeout:
            target_addr = self._get_target_address()
            try:
                async with grpc.aio.insecure_channel(target_addr) as channel:
                    stub = raft_pb2_grpc.ClientServiceStub(channel)
                    req = raft_pb2.DeleteArgs(
                        key=key,
                        client_id=self.client_id,
                        sequence_num=seq
                    )
                    reply: raft_pb2.DeleteReply = await stub.Delete(req, timeout=2.0)

                    if reply.success:
                        logger.info(f"DELETE key='{key}' succeeded on {target_addr}.")
                        return True

                    if reply.error == "NOT_LEADER":
                        if reply.leader_address:
                            self.known_leader_addr = reply.leader_address
                        else:
                            self.known_leader_addr = None
                            self.leader_index += 1
                    else:
                        await asyncio.sleep(0.2)

            except Exception as e:
                self.known_leader_addr = None
                self.leader_index += 1
                await asyncio.sleep(0.3)

        return False

async def cli_main():
    parser = argparse.ArgumentParser(description="RaftKV Smart Client CLI")
    parser.add_argument("op", choices=["put", "get", "delete"], help="Operation to perform")
    parser.add_argument("key", help="Target Key")
    parser.add_argument("value", nargs="?", default="", help="Target Value (for put)")
    parser.add_argument("--cluster", default="127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053,127.0.0.1:50054,127.0.0.1:50055", help="Comma-separated cluster addresses")

    args = parser.parse_args()
    addresses = args.cluster.split(",")
    client = RaftKVClient(addresses)

    if args.op == "put":
        success = await client.put(args.key, args.value)
        sys.exit(0 if success else 1)
    elif args.op == "get":
        found, val = await client.get(args.key)
        if found:
            print(f"KEY: {args.key} = '{val}'")
            sys.exit(0)
        else:
            print(f"KEY: {args.key} NOT FOUND")
            sys.exit(1)
    elif args.op == "delete":
        success = await client.delete(args.key)
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    asyncio.run(cli_main())
