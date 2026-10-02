import os
import shutil
import tempfile
import pytest
from raftkv.storage.wal import WAL
from raftkv.proto import raft_pb2

@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)

def test_wal_metadata(temp_dir):
    db_path = os.path.join(temp_dir, "test_wal.sqlite")
    wal = WAL(db_path)

    # Initial state
    term, voted, l_idx, l_term, entries = wal.load_state()
    assert term == 0
    assert voted is None
    assert l_idx == 0
    assert entries == []

    # Update metadata
    wal.save_metadata(2, "node2")
    term, voted, _, _, _ = wal.load_state()
    assert term == 2
    assert voted == "node2"

    wal.close()

def test_wal_append_and_truncate(temp_dir):
    db_path = os.path.join(temp_dir, "test_wal.sqlite")
    wal = WAL(db_path)

    e1 = raft_pb2.LogEntry(index=1, term=1, op=raft_pb2.OpType.PUT, key="k1", value="v1")
    e2 = raft_pb2.LogEntry(index=2, term=1, op=raft_pb2.OpType.PUT, key="k2", value="v2")
    e3 = raft_pb2.LogEntry(index=3, term=2, op=raft_pb2.OpType.DELETE, key="k1")

    wal.append_entries([e1, e2, e3])

    _, _, _, _, entries = wal.load_state()
    assert len(entries) == 3
    assert entries[0].key == "k1"
    assert entries[2].op == raft_pb2.OpType.DELETE

    # Truncate from index 2
    wal.truncate_log_from(2)
    _, _, _, _, entries = wal.load_state()
    assert len(entries) == 1
    assert entries[0].index == 1

    wal.close()

def test_wal_compact(temp_dir):
    db_path = os.path.join(temp_dir, "test_wal.sqlite")
    wal = WAL(db_path)

    entries = [
        raft_pb2.LogEntry(index=i, term=1, op=raft_pb2.OpType.PUT, key=f"k{i}", value=f"v{i}")
        for i in range(1, 6)
    ]
    wal.append_entries(entries)

    wal.compact_log_up_to(3)
    wal.save_snapshot_metadata(3, 1)

    _, _, l_idx, l_term, remaining = wal.load_state()
    assert l_idx == 3
    assert l_term == 1
    assert len(remaining) == 2
    assert remaining[0].index == 4

    wal.close()
