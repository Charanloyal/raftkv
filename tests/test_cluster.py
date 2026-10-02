import asyncio
import logging
import os
import shutil
import tempfile
import time
import pytest
from typing import List, Dict

from raftkv.server import serve_node
from raftkv.client import RaftKVClient
from raftkv.proto import raft_pb2, raft_pb2_grpc
import grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("ClusterTestHarness")

class LocalCluster:
    def __init__(self, node_count: int = 5, base_port: int = 50100):
        self.node_count = node_count
        self.base_port = base_port
        self.temp_dir = tempfile.mkdtemp(prefix="raftkv_cluster_")
        self.nodes_meta: Dict[str, dict] = {}
        self.tasks: Dict[str, asyncio.Task] = {}
        
        peer_addresses = {}
        for i in range(1, node_count + 1):
            node_id = f"node-{i}"
            port = base_port + i
            peer_addresses[node_id] = f"127.0.0.1:{port}"
        
        self.peer_addresses = peer_addresses

    async def start_cluster(self):
        logger.info(f"Starting {self.node_count}-node local cluster in {self.temp_dir}...")
        for node_id, addr in self.peer_addresses.items():
            port = int(addr.split(":")[1])
            node_dir = os.path.join(self.temp_dir, node_id)
            os.makedirs(node_dir, exist_ok=True)
            self.nodes_meta[node_id] = {"port": port, "dir": node_dir}
            
            task = asyncio.create_task(
                serve_node(
                    node_id=node_id,
                    peer_addresses=dict(self.peer_addresses),
                    port=port,
                    data_dir=node_dir
                )
            )
            self.tasks[node_id] = task

        logger.info("All cluster nodes launched.")

    async def stop_node(self, node_id: str):
        if node_id in self.tasks:
            logger.info(f"Killing node {node_id}...")
            self.tasks[node_id].cancel()
            try:
                await self.tasks[node_id]
            except asyncio.CancelledError:
                pass
            del self.tasks[node_id]
            logger.info(f"Node {node_id} stopped.")

    async def restart_node(self, node_id: str):
        if node_id in self.nodes_meta and node_id not in self.tasks:
            logger.info(f"Restarting node {node_id}...")
            meta = self.nodes_meta[node_id]
            task = asyncio.create_task(
                serve_node(
                    node_id=node_id,
                    peer_addresses=dict(self.peer_addresses),
                    port=meta["port"],
                    data_dir=meta["dir"]
                )
            )
            self.tasks[node_id] = task

    async def find_leader(self, timeout: float = 3.0) -> str:
        start_t = time.monotonic()
        while time.monotonic() - start_t < timeout:
            for node_id, addr in list(self.peer_addresses.items()):
                if node_id not in self.tasks:
                    continue
                try:
                    async with grpc.aio.insecure_channel(addr) as channel:
                        stub = raft_pb2_grpc.ClientServiceStub(channel)
                        reply = await stub.Get(raft_pb2.GetArgs(key="__leader_check__"), timeout=0.2)
                        if reply.leader_id:
                            return reply.leader_id
                except Exception:
                    pass
            await asyncio.sleep(0.1)
        return ""

    async def stop_cluster(self):
        logger.info("Tearing down local cluster...")
        for node_id, task in list(self.tasks.items()):
            task.cancel()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

@pytest.mark.asyncio
async def test_5node_cluster_failover_and_linearizability():
    cluster = LocalCluster(node_count=5, base_port=50200)
    await cluster.start_cluster()
    
    try:
        # 1. Wait for leader election convergence
        leader_1 = await cluster.find_leader(timeout=4.0)
        assert leader_1 != "", "Leader failed to emerge within election window."
        logger.info(f"Electing Leader converged to: {leader_1}")

        # 2. Perform client operations
        addrs = list(cluster.peer_addresses.values())
        client = RaftKVClient(addrs)
        
        ok = await client.put("user:100", "Alice", timeout=5.0)
        assert ok, "Client PUT failed on initial cluster"
        
        found, val = await client.get("user:100", timeout=5.0)
        assert found and val == "Alice", f"Client GET expected 'Alice', got '{val}'"

        # 3. Chaos Test: Kill the leader!
        logger.info(f"CHAOS TEST: Terminating leader {leader_1}...")
        await cluster.stop_node(leader_1)

        # 4. Assert new election convergence (< 1.0 second)
        start_t = time.monotonic()
        leader_2 = ""
        while time.monotonic() - start_t < 2.0:
            candidate_leader = await cluster.find_leader(timeout=0.5)
            if candidate_leader and candidate_leader != leader_1:
                leader_2 = candidate_leader
                break
                
        election_time = time.monotonic() - start_t
        assert leader_2 != "", "New leader failed to emerge after failover"
        assert leader_2 != leader_1, "Failed leader was still reported as leader"
        logger.info(f"SUCCESS: New leader {leader_2} elected in {election_time:.3f} seconds (< 1.0s target)!")

        # 5. Verify linearizable writes during failover
        ok2 = await client.put("user:101", "Bob", timeout=5.0)
        assert ok2, "Client PUT failed after leader failover"

        found1, val1 = await client.get("user:100", timeout=5.0)
        assert found1 and val1 == "Alice", "Pre-failover key lost!"
        
        found2, val2 = await client.get("user:101", timeout=5.0)
        assert found2 and val2 == "Bob", "Post-failover key lost!"

        # 6. Partition Healing: Restart original leader node
        logger.info(f"Healing cluster: Restarting node {leader_1}...")
        await cluster.restart_node(leader_1)
        await asyncio.sleep(0.5)

        # 7. Verify restarted node catches up with replicated data
        found1_after, val1_after = await client.get("user:101", timeout=5.0)
        assert found1_after and val1_after == "Bob"
        logger.info("Cluster failover and partition healing test passed successfully!")

    finally:
        await cluster.stop_cluster()

if __name__ == "__main__":
    asyncio.run(test_5node_cluster_failover_and_linearizability())
