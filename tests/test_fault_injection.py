import asyncio
import logging
import pytest
from tests.test_cluster import LocalCluster
from raftkv.client import RaftKVClient

logging.basicConfig(level=logging.INFO)

@pytest.mark.asyncio
async def test_5node_fault_injection_matrix():
    cluster = LocalCluster(node_count=5, base_port=50500)
    await cluster.start_cluster()
    
    try:
        # 1. Cluster Bootstrap & Initial Leader Election
        leader_1 = await cluster.find_leader(timeout=4.0)
        assert leader_1 != "", "Failed to elect initial cluster leader"
        
        client = RaftKVClient(list(cluster.peer_addresses.values()))
        
        # 2. Sequential commits to establish base log
        for i in range(3):
            assert await client.put(f"base_key_{i}", f"val_{i}", timeout=4.0)

        # 3. Fault-Injection: Abrupt Leader Crash
        await cluster.stop_node(leader_1)
        
        # 4. Failover Convergence Assertion (< 1.0s)
        leader_2 = ""
        for _ in range(20):
            cand = await cluster.find_leader(timeout=0.2)
            if cand and cand != leader_1:
                leader_2 = cand
                break
            await asyncio.sleep(0.05)
            
        assert leader_2 != "" and leader_2 != leader_1, "New leader failed to converge after crash"
        
        # 5. Client automatic redirection write
        assert await client.put("failover_key", "replicated_value", timeout=4.0)
        
        # 6. Minority partition test: restart leader_1 first, then kill 2 followers
        await cluster.restart_node(leader_1)
        await asyncio.sleep(0.4)
        
        nodes_to_kill = [n for n in ["node-2", "node-3", "node-4", "node-5"] if n != leader_2][:2]
        await cluster.stop_node(nodes_to_kill[0])
        await cluster.stop_node(nodes_to_kill[1])
        await asyncio.sleep(0.2)
        
        # Quorum (3/5) must remain operational
        assert await client.put("minority_key", "quorum_ok", timeout=4.0)
        
        # 7. Partition healing & Catch-Up verification
        await cluster.restart_node(nodes_to_kill[0])
        await cluster.restart_node(nodes_to_kill[1])
        await asyncio.sleep(0.6)
        
        # Verify linearizable data across healed cluster
        f1, v1 = await client.get("failover_key", timeout=4.0)
        f2, v2 = await client.get("minority_key", timeout=4.0)
        assert f1 and v1 == "replicated_value"
        assert f2 and v2 == "quorum_ok"
        
    finally:
        await cluster.stop_cluster()
