import os
import shutil
import tempfile
import asyncio
import pytest
from raftkv.consensus.engine import RaftEngine, NodeRole
from raftkv.proto import raft_pb2

@pytest.fixture
def temp_dirs():
    dirs = [tempfile.mkdtemp() for _ in range(3)]
    yield dirs
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)

@pytest.mark.asyncio
async def test_raft_engine_initial_state(temp_dirs):
    peers = {
        "node-1": "127.0.0.1:50051",
        "node-2": "127.0.0.1:50052",
        "node-3": "127.0.0.1:50053"
    }
    engine = RaftEngine("node-1", peers, temp_dirs[0])
    assert engine.role == NodeRole.FOLLOWER
    assert engine.current_term == 0
    assert engine.voted_for is None
    await engine.stop()

@pytest.mark.asyncio
async def test_request_vote_handler(temp_dirs):
    peers = {"node-1": "127.0.0.1:50051", "node-2": "127.0.0.1:50052"}
    engine = RaftEngine("node-1", peers, temp_dirs[0])

    engine.current_term = 1

    # 1. Lower term request -> rejected
    req1 = raft_pb2.RequestVoteArgs(term=0, candidate_id="node-2", last_log_index=1, last_log_term=1)
    rep1 = await engine.handle_request_vote(req1)
    assert not rep1.vote_granted

    # 2. Higher term request -> granted
    req2 = raft_pb2.RequestVoteArgs(term=2, candidate_id="node-2", last_log_index=0, last_log_term=0)
    rep2 = await engine.handle_request_vote(req2)
    assert rep2.vote_granted
    assert engine.current_term == 2
    assert engine.voted_for == "node-2"

    # 3. Second request in same term -> rejected
    req3 = raft_pb2.RequestVoteArgs(term=2, candidate_id="node-3", last_log_index=0, last_log_term=0)
    rep3 = await engine.handle_request_vote(req3)
    assert not rep3.vote_granted

    await engine.stop()

@pytest.mark.asyncio
async def test_append_entries_log_replication(temp_dirs):
    peers = {"node-1": "127.0.0.1:50051", "node-2": "127.0.0.1:50052"}
    engine = RaftEngine("node-1", peers, temp_dirs[0])

    entry1 = raft_pb2.LogEntry(index=1, term=1, op=raft_pb2.OpType.PUT, key="foo", value="bar")
    args = raft_pb2.AppendEntriesArgs(
        term=1,
        leader_id="node-2",
        prev_log_index=0,
        prev_log_term=0,
        entries=[entry1],
        leader_commit=1
    )

    reply = await engine.handle_append_entries(args)
    assert reply.success
    assert reply.match_index == 1
    assert engine.commit_index == 1
    assert len(engine.log) == 1

    await engine.stop()
