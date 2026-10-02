import os
import json
import tempfile
from typing import Dict, Any, Optional, Tuple

class SnapshotManager:
    """
    Manages atomic snapshot creation and loading for state machine recovery.
    """
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.snapshot_path = os.path.join(self.data_dir, "snapshot.json")

    def save_snapshot(
        self, 
        last_included_index: int, 
        last_included_term: int, 
        kv_store: Dict[str, str], 
        last_seq: Dict[str, int]
    ) -> bytes:
        snapshot_payload = {
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
            "kv_store": kv_store,
            "last_seq": last_seq
        }
        data = json.dumps(snapshot_payload).encode("utf-8")
        
        # Atomic write via temp file
        temp_fd, temp_path = tempfile.mkstemp(dir=self.data_dir, suffix=".tmp")
        try:
            with os.fdopen(temp_fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.snapshot_path)
        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise
            
        return data

    def load_snapshot(self) -> Optional[Tuple[int, int, Dict[str, str], Dict[str, int]]]:
        if not os.path.exists(self.snapshot_path):
            return None
        try:
            with open(self.snapshot_path, "rb") as f:
                data = json.loads(f.read().decode("utf-8"))
            return (
                data["last_included_index"],
                data["last_included_term"],
                data.get("kv_store", {}),
                data.get("last_seq", {})
            )
        except Exception as e:
            print(f"[SnapshotManager] Error loading snapshot: {e}")
            return None

    def read_snapshot_bytes(self) -> bytes:
        if not os.path.exists(self.snapshot_path):
            return b""
        with open(self.snapshot_path, "rb") as f:
            return f.read()

    def write_snapshot_bytes(self, snapshot_bytes: bytes) -> Optional[Tuple[int, int, Dict[str, str], Dict[str, int]]]:
        if not snapshot_bytes:
            return None
        try:
            data = json.loads(snapshot_bytes.decode("utf-8"))
            # Atomic save
            temp_fd, temp_path = tempfile.mkstemp(dir=self.data_dir, suffix=".tmp")
            with os.fdopen(temp_fd, "wb") as f:
                f.write(snapshot_bytes)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.snapshot_path)
            
            return (
                data["last_included_index"],
                data["last_included_term"],
                data.get("kv_store", {}),
                data.get("last_seq", {})
            )
        except Exception as e:
            print(f"[SnapshotManager] Error writing snapshot bytes: {e}")
            return None
