import logging
from typing import Dict, Any, Optional, Tuple
from raftkv.proto import raft_pb2
from raftkv.storage.snapshot import SnapshotManager

logger = logging.getLogger(__name__)

class KeyValueStateMachine:
    """
    In-memory strongly consistent Key-Value State Machine with Client Request Deduplication.
    Integrated with SnapshotManager for log compaction and crash recovery.
    """
    def __init__(self, snapshot_manager: SnapshotManager):
        self.snapshot_manager = snapshot_manager
        self.kv_store: Dict[str, str] = {}
        # Client ID -> highest executed sequence number
        self.last_seq: Dict[str, int] = {}
        self.last_applied_index: int = 0
        self.last_applied_term: int = 0

    def apply(self, entry: raft_pb2.LogEntry) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Applies a log entry to the state machine atomically.
        Returns (success, result_value, error_message).
        """
        self.last_applied_index = entry.index
        self.last_applied_term = entry.term

        # Check for client deduplication (at-most-once / exactly-once execution)
        if entry.client_id and entry.sequence_num > 0:
            last_seen = self.last_seq.get(entry.client_id, 0)
            if entry.sequence_num <= last_seen:
                logger.info(f"Duplicate client request skipped. Client: {entry.client_id}, Seq: {entry.sequence_num}")
                if entry.op == raft_pb2.OpType.GET:
                    val = self.kv_store.get(entry.key, "")
                    found = entry.key in self.kv_store
                    return found, val, None
                return True, self.kv_store.get(entry.key, None), None

        result_val: Optional[str] = None
        found: bool = True

        if entry.op == raft_pb2.OpType.PUT:
            self.kv_store[entry.key] = entry.value
            result_val = entry.value
            logger.debug(f"[StateMachine] PUT key='{entry.key}' val='{entry.value}' at index={entry.index}")

        elif entry.op == raft_pb2.OpType.DELETE:
            if entry.key in self.kv_store:
                del self.kv_store[entry.key]
            logger.debug(f"[StateMachine] DELETE key='{entry.key}' at index={entry.index}")

        elif entry.op == raft_pb2.OpType.GET:
            result_val = self.kv_store.get(entry.key, None)
            found = entry.key in self.kv_store

        elif entry.op == raft_pb2.OpType.NOOP:
            logger.debug(f"[StateMachine] NOOP applied at index={entry.index}")

        if entry.client_id and entry.sequence_num > 0:
            self.last_seq[entry.client_id] = max(self.last_seq.get(entry.client_id, 0), entry.sequence_num)

        return found, result_val, None

    def get(self, key: str) -> Tuple[bool, str]:
        if key in self.kv_store:
            return True, self.kv_store[key]
        return False, ""

    def take_snapshot(self, last_included_index: int, last_included_term: int) -> bytes:
        return self.snapshot_manager.save_snapshot(
            last_included_index=last_included_index,
            last_included_term=last_included_term,
            kv_store=dict(self.kv_store),
            last_seq=dict(self.last_seq)
        )

    def restore_from_snapshot(self, snapshot_bytes: bytes) -> Optional[Tuple[int, int]]:
        res = self.snapshot_manager.write_snapshot_bytes(snapshot_bytes)
        if res is None:
            return None
        last_idx, last_term, kv_store, last_seq = res
        self.kv_store = kv_store
        self.last_seq = last_seq
        self.last_applied_index = last_idx
        self.last_applied_term = last_term
        logger.info(f"[StateMachine] Restored snapshot: last_idx={last_idx}, keys_count={len(kv_store)}")
        return last_idx, last_term

    def load_existing_snapshot(self) -> Optional[Tuple[int, int]]:
        res = self.snapshot_manager.load_snapshot()
        if res is None:
            return None
        last_idx, last_term, kv_store, last_seq = res
        self.kv_store = kv_store
        self.last_seq = last_seq
        self.last_applied_index = last_idx
        self.last_applied_term = last_term
        logger.info(f"[StateMachine] Loaded existing snapshot from disk: last_idx={last_idx}, keys={len(kv_store)}")
        return last_idx, last_term
