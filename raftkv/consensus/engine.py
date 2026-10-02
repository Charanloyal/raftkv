import asyncio
import enum
import logging
import random
import time
from typing import Dict, List, Optional, Tuple, Any

from raftkv.proto import raft_pb2
from raftkv.storage.wal import WAL
from raftkv.storage.snapshot import SnapshotManager
from raftkv.storage.state_machine import KeyValueStateMachine
from raftkv.consensus.rpc_client import PeerRPCClient

logger = logging.getLogger(__name__)

class NodeRole(enum.Enum):
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"

class RaftEngine:
    """
    Complete, strictly compliant Raft Consensus Engine.
    Handles Leader Elections, Log Replication, Snapshotting, and State Machine Application.
    """
    def __init__(
        self,
        node_id: str,
        peer_addresses: Dict[str, str],
        data_dir: str,
        snapshot_threshold: int = 50
    ):
        self.node_id = node_id
        # Filter out self from peers dict
        self.peers = {k: v for k, v in peer_addresses.items() if k != node_id}
        self.data_dir = data_dir
        self.snapshot_threshold = snapshot_threshold

        self.lock = asyncio.Lock()
        self.role = NodeRole.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None

        # Persistent storage & State Machine initialization
        wal_path = f"{data_dir}/wal_{node_id}.sqlite"
        self.wal = WAL(wal_path)
        self.snapshot_manager = SnapshotManager(data_dir)
        self.state_machine = KeyValueStateMachine(self.snapshot_manager)

        # Log & Snapshot state
        self.log: List[raft_pb2.LogEntry] = []
        self.last_included_index: int = 0
        self.last_included_term: int = 0

        # Volatile state on all nodes
        self.commit_index: int = 0
        self.last_applied: int = 0

        # Volatile state on Leader
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Timers & Tasks
        self.last_heartbeat_rx = time.monotonic()
        self.election_timeout = random.uniform(0.150, 0.300)
        self.is_running = False
        
        self.rpc_client = PeerRPCClient(self.peers)
        
        # Pending client requests map: log_index -> asyncio.Future
        self.pending_requests: Dict[int, asyncio.Future] = {}

        self._load_persistent_state()

    def _load_persistent_state(self):
        # 1. Load state machine snapshot if present
        snapshot_meta = self.state_machine.load_existing_snapshot()
        if snapshot_meta:
            self.last_included_index, self.last_included_term = snapshot_meta
            self.commit_index = self.last_included_index
            self.last_applied = self.last_included_index

        # 2. Load WAL persistent state
        term, voted, l_idx, l_term, entries = self.wal.load_state()
        self.current_term = term
        self.voted_for = voted
        if l_idx > self.last_included_index:
            self.last_included_index = l_idx
            self.last_included_term = l_term
            self.commit_index = max(self.commit_index, l_idx)
            self.last_applied = max(self.last_applied, l_idx)
            
        self.log = entries
        logger.info(
            f"[{self.node_id}] State restored: Term={self.current_term}, VotedFor={self.voted_for}, "
            f"LogLen={len(self.log)}, LastIncludedIndex={self.last_included_index}"
        )

    # --- Log Index Helpers ---

    def _get_last_log_index(self) -> int:
        if self.log:
            return self.log[-1].index
        return self.last_included_index

    def _get_last_log_term(self) -> int:
        if self.log:
            return self.log[-1].term
        return self.last_included_term

    def _get_entry(self, index: int) -> Optional[raft_pb2.LogEntry]:
        if index <= self.last_included_index:
            return None
        array_idx = index - self.last_included_index - 1
        if 0 <= array_idx < len(self.log):
            return self.log[array_idx]
        return None

    def _get_term_at(self, index: int) -> int:
        if index == 0:
            return 0
        if index == self.last_included_index:
            return self.last_included_term
        entry = self._get_entry(index)
        return entry.term if entry else 0

    # --- Engine Lifecycle ---

    async def start(self):
        self.is_running = True
        self.last_heartbeat_rx = time.monotonic()
        asyncio.create_task(self._election_timer_loop())
        asyncio.create_task(self._heartbeat_loop())
        asyncio.create_task(self._apply_loop())
        logger.info(f"[{self.node_id}] Raft Engine started successfully as {self.role.value}.")

    async def stop(self):
        self.is_running = False
        await self.rpc_client.close()
        self.wal.close()
        logger.info(f"[{self.node_id}] Raft Engine stopped.")

    # --- Background Loops ---

    async def _election_timer_loop(self):
        while self.is_running:
            await asyncio.sleep(0.015)
            async with self.lock:
                if self.role == NodeRole.LEADER:
                    continue
                elapsed = time.monotonic() - self.last_heartbeat_rx
                if elapsed >= self.election_timeout:
                    logger.info(f"[{self.node_id}] Election timeout ({elapsed:.3f}s >= {self.election_timeout:.3f}s). Initiating election!")
                    await self._start_election()

    async def _start_election(self):
        self.role = NodeRole.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self.wal.save_metadata(self.current_term, self.voted_for)
        self.last_heartbeat_rx = time.monotonic()
        self.election_timeout = random.uniform(0.150, 0.300)

        term = self.current_term
        last_log_idx = self._get_last_log_index()
        last_log_term = self._get_last_log_term()
        peers = list(self.peers.keys())

        logger.info(f"[{self.node_id}] Candidate in Term {term}. Requesting votes from peers: {peers}")

        args = raft_pb2.RequestVoteArgs(
            term=term,
            candidate_id=self.node_id,
            last_log_index=last_log_idx,
            last_log_term=last_log_term
        )

        votes_granted = 1
        majority = ((len(self.peers) + 1) // 2) + 1

        async def request_peer_vote(peer_id: str):
            nonlocal votes_granted
            reply = await self.rpc_client.send_request_vote(peer_id, args, timeout=0.15)
            if not reply:
                return
            async with self.lock:
                if self.role != NodeRole.CANDIDATE or self.current_term != term:
                    return
                if reply.term > self.current_term:
                    logger.info(f"[{self.node_id}] Discovered higher term {reply.term} from {peer_id}. Stepping down to Follower.")
                    self.current_term = reply.term
                    self.role = NodeRole.FOLLOWER
                    self.voted_for = None
                    self.wal.save_metadata(self.current_term, None)
                    return
                if reply.vote_granted:
                    votes_granted += 1
                    logger.info(f"[{self.node_id}] Received vote from {peer_id} ({votes_granted}/{(len(self.peers) + 1)})")
                    if votes_granted >= majority and self.role == NodeRole.CANDIDATE:
                        await self._become_leader()

        for p in peers:
            asyncio.create_task(request_peer_vote(p))

    async def _become_leader(self):
        if self.role == NodeRole.LEADER:
            return
        self.role = NodeRole.LEADER
        self.leader_id = self.node_id
        last_log_idx = self._get_last_log_index()
        
        for p in self.peers:
            self.next_index[p] = last_log_idx + 1
            self.match_index[p] = 0

        logger.info(f"[{self.node_id}] *** ELECTED LEADER FOR TERM {self.current_term} ***")

        # Append NOOP entry to establish leader log commitment in current term
        noop_entry = raft_pb2.LogEntry(
            index=last_log_idx + 1,
            term=self.current_term,
            op=raft_pb2.OpType.NOOP
        )
        self.log.append(noop_entry)
        self.wal.append_entries([noop_entry])

        await self._send_heartbeats()

    async def _heartbeat_loop(self):
        while self.is_running:
            await asyncio.sleep(0.050)
            async with self.lock:
                if self.role == NodeRole.LEADER:
                    await self._send_heartbeats()

    async def _send_heartbeats(self):
        for peer_id in self.peers:
            asyncio.create_task(self._replicate_to_peer(peer_id))

    async def _replicate_to_peer(self, peer_id: str):
        async with self.lock:
            if self.role != NodeRole.LEADER:
                return
            
            term = self.current_term
            prev_log_index = self.next_index.get(peer_id, self._get_last_log_index() + 1) - 1

            # Check if peer needs InstallSnapshot instead
            if prev_log_index < self.last_included_index:
                await self._send_snapshot_to_peer(peer_id)
                return

            prev_log_term = self._get_term_at(prev_log_index)
            
            # Gather log entries to replicate
            entries_to_send = []
            if prev_log_index >= self.last_included_index:
                start_array_idx = prev_log_index - self.last_included_index
                entries_to_send = list(self.log[start_array_idx:])

            args = raft_pb2.AppendEntriesArgs(
                term=term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries_to_send,
                leader_commit=self.commit_index
            )

        reply = await self.rpc_client.send_append_entries(peer_id, args, timeout=0.10)
        if not reply:
            return

        async with self.lock:
            if self.role != NodeRole.LEADER or self.current_term != term:
                return
            
            if reply.term > self.current_term:
                logger.info(f"[{self.node_id}] Heartbeat reply from {peer_id} has higher term {reply.term}. Stepping down to Follower.")
                self.current_term = reply.term
                self.role = NodeRole.FOLLOWER
                self.voted_for = None
                self.leader_id = None
                self.wal.save_metadata(self.current_term, None)
                return

            if reply.success:
                self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), reply.match_index)
                self.next_index[peer_id] = self.match_index[peer_id] + 1
                await self._update_commit_index()
            else:
                # Fast rewind next_index on conflict
                if reply.conflict_index > 0:
                    self.next_index[peer_id] = min(reply.conflict_index, self.next_index.get(peer_id, 1) - 1)
                else:
                    self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)

    async def _send_snapshot_to_peer(self, peer_id: str):
        term = self.current_term
        last_idx = self.last_included_index
        last_term = self.last_included_term
        snapshot_bytes = self.snapshot_manager.read_snapshot_bytes()

        args = raft_pb2.SnapshotArgs(
            term=term,
            leader_id=self.node_id,
            last_included_index=last_idx,
            last_included_term=last_term,
            offset=0,
            data=snapshot_bytes,
            done=True
        )

        reply = await self.rpc_client.send_install_snapshot(peer_id, args, timeout=0.50)
        if reply and reply.term > self.current_term:
            self.current_term = reply.term
            self.role = NodeRole.FOLLOWER
            self.voted_for = None
            self.leader_id = None
            self.wal.save_metadata(self.current_term, None)
        elif reply:
            self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), last_idx)
            self.next_index[peer_id] = self.match_index[peer_id] + 1

    async def _update_commit_index(self):
        """
        Raft Leader Commit Rule:
        If there exists N > commitIndex such that majority of matchIndex[i] >= N
        and log[N].term == currentTerm, set commitIndex = N.
        """
        if self.role != NodeRole.LEADER:
            return

        match_indices = sorted(list(self.match_index.values()) + [self._get_last_log_index()])
        # Median index represents majority agreement
        majority_idx = (len(self.peers) + 1) // 2
        median_N = match_indices[len(match_indices) - 1 - majority_idx]

        if median_N > self.commit_index:
            if self._get_term_at(median_N) == self.current_term:
                logger.info(f"[{self.node_id}] Leader advancing commit_index: {self.commit_index} -> {median_N}")
                self.commit_index = median_N

    async def _apply_loop(self):
        while self.is_running:
            await asyncio.sleep(0.005)
            to_apply = []
            async with self.lock:
                while self.commit_index > self.last_applied:
                    self.last_applied += 1
                    entry = self._get_entry(self.last_applied)
                    if entry:
                        to_apply.append(entry)

            for entry in to_apply:
                found, val, err = self.state_machine.apply(entry)
                
                # Fulfill waiting client future if present
                fut = self.pending_requests.pop(entry.index, None)
                if fut and not fut.done():
                    fut.set_result((found, val, err))

            # Check snapshot compaction threshold
            async with self.lock:
                if len(self.log) >= self.snapshot_threshold:
                    await self._compact_snapshot()

    async def _compact_snapshot(self):
        if not self.log:
            return
        compact_up_to = self.last_applied
        if compact_up_to <= self.last_included_index:
            return

        compact_term = self._get_term_at(compact_up_to)
        logger.info(f"[{self.node_id}] Triggering snapshot compaction up to index {compact_up_to} (term {compact_term})")

        # Dump snapshot
        self.state_machine.take_snapshot(compact_up_to, compact_term)
        
        # Truncate in-memory log
        cut_offset = compact_up_to - self.last_included_index
        self.log = self.log[cut_offset:]
        self.last_included_index = compact_up_to
        self.last_included_term = compact_term

        # Update WAL
        self.wal.compact_log_up_to(compact_up_to)
        self.wal.save_snapshot_metadata(compact_up_to, compact_term)

    # --- RPC Handlers ---

    async def handle_request_vote(self, args: raft_pb2.RequestVoteArgs) -> raft_pb2.RequestVoteReply:
        async with self.lock:
            reply = raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=False)
            
            if args.term < self.current_term:
                return reply

            if args.term > self.current_term:
                self.current_term = args.term
                self.role = NodeRole.FOLLOWER
                self.voted_for = None
                self.leader_id = None
                self.wal.save_metadata(self.current_term, None)

            # Raft log up-to-date check
            last_log_idx = self._get_last_log_index()
            last_log_term = self._get_last_log_term()

            log_ok = (args.last_log_term > last_log_term) or (
                args.last_log_term == last_log_term and args.last_log_index >= last_log_idx
            )

            if (self.voted_for is None or self.voted_for == args.candidate_id) and log_ok:
                self.voted_for = args.candidate_id
                self.wal.save_metadata(self.current_term, self.voted_for)
                self.last_heartbeat_rx = time.monotonic()
                reply.vote_granted = True
                logger.info(f"[{self.node_id}] Granted vote to {args.candidate_id} in term {args.term}")

            reply.term = self.current_term
            return reply

    async def handle_append_entries(self, args: raft_pb2.AppendEntriesArgs) -> raft_pb2.AppendEntriesReply:
        async with self.lock:
            reply = raft_pb2.AppendEntriesReply(
                term=self.current_term,
                success=False,
                match_index=0,
                conflict_index=0,
                conflict_term=0
            )

            if args.term < self.current_term:
                return reply

            if args.term > self.current_term or self.role == NodeRole.CANDIDATE:
                self.current_term = args.term
                self.role = NodeRole.FOLLOWER
                self.voted_for = None
                self.wal.save_metadata(self.current_term, None)

            self.leader_id = args.leader_id
            self.last_heartbeat_rx = time.monotonic()

            # Verify log consistency at prev_log_index
            last_log_idx = self._get_last_log_index()
            if args.prev_log_index > last_log_idx:
                reply.conflict_index = last_log_idx + 1
                return reply

            if args.prev_log_index > 0:
                prev_term = self._get_term_at(args.prev_log_index)
                if prev_term != args.prev_log_term:
                    reply.conflict_term = prev_term
                    # Find first index for conflict term
                    idx = args.prev_log_index
                    while idx > self.last_included_index and self._get_term_at(idx) == prev_term:
                        idx -= 1
                    reply.conflict_index = idx + 1
                    return reply

            # Log matching passed. Append new entries.
            new_entries_to_append = []
            for entry in args.entries:
                if entry.index <= self.last_included_index:
                    continue
                
                existing_entry = self._get_entry(entry.index)
                if existing_entry:
                    if existing_entry.term != entry.term:
                        # Conflict: truncate existing entry & following entries
                        cut_idx = entry.index - self.last_included_index - 1
                        self.log = self.log[:cut_idx]
                        self.wal.truncate_log_from(entry.index)
                        new_entries_to_append.append(entry)
                else:
                    new_entries_to_append.append(entry)

            if new_entries_to_append:
                self.log.extend(new_entries_to_append)
                self.wal.append_entries(new_entries_to_append)

            # Update follower commit index
            if args.leader_commit > self.commit_index:
                self.commit_index = min(args.leader_commit, self._get_last_log_index())

            reply.success = True
            reply.match_index = self._get_last_log_index()
            reply.term = self.current_term
            return reply

    async def handle_install_snapshot(self, args: raft_pb2.SnapshotArgs) -> raft_pb2.SnapshotReply:
        async with self.lock:
            reply = raft_pb2.SnapshotReply(term=self.current_term)
            if args.term < self.current_term:
                return reply

            if args.term > self.current_term:
                self.current_term = args.term
                self.role = NodeRole.FOLLOWER
                self.voted_for = None
                self.wal.save_metadata(self.current_term, None)

            self.leader_id = args.leader_id
            self.last_heartbeat_rx = time.monotonic()

            if args.last_included_index <= self.last_included_index:
                return reply

            # Restore state machine from snapshot
            res = self.state_machine.restore_from_snapshot(args.data)
            if res:
                self.last_included_index = args.last_included_index
                self.last_included_term = args.last_included_term
                self.log.clear()
                self.wal.clear_all_log()
                self.wal.save_snapshot_metadata(args.last_included_index, args.last_included_term)
                self.commit_index = max(self.commit_index, args.last_included_index)
                self.last_applied = max(self.last_applied, args.last_included_index)
                logger.info(f"[{self.node_id}] Installed snapshot up to index {args.last_included_index}")

            reply.term = self.current_term
            return reply

    # --- Client Command Execution ---

    async def execute_client_command(
        self, op: raft_pb2.OpType, key: str, value: str, client_id: str, seq: int
    ) -> Tuple[bool, Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Executes a client request through Raft consensus.
        Returns (success, result_value, error_msg, leader_id, leader_addr).
        """
        async with self.lock:
            if self.role != NodeRole.LEADER:
                leader_addr = self.peers.get(self.leader_id, "") if self.leader_id else ""
                return False, None, "NOT_LEADER", self.leader_id, leader_addr

            index = self._get_last_log_index() + 1
            term = self.current_term

            entry = raft_pb2.LogEntry(
                index=index,
                term=term,
                op=op,
                key=key,
                value=value,
                client_id=client_id,
                sequence_num=seq
            )

            self.log.append(entry)
            self.wal.append_entries([entry])

            fut = asyncio.get_event_loop().create_future()
            self.pending_requests[index] = fut

            # Replicate immediately
            await self._send_heartbeats()

        try:
            # Wait for consensus commit and state machine execution
            found, result_val, err = await asyncio.wait_for(fut, timeout=5.0)
            return True, result_val, err, self.node_id, ""
        except asyncio.TimeoutError:
            self.pending_requests.pop(index, None)
            return False, None, "REQUEST_TIMEOUT", self.node_id, ""

    async def read_client_command(self, key: str) -> Tuple[bool, Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Executes a read request with Leader verification for linearizability.
        """
        async with self.lock:
            if self.role != NodeRole.LEADER:
                leader_addr = self.peers.get(self.leader_id, "") if self.leader_id else ""
                return False, None, "NOT_LEADER", self.leader_id, leader_addr

        # Serve read from local state machine
        found, val = self.state_machine.get(key)
        return True, val if found else "", None if found else "KEY_NOT_FOUND", self.node_id, ""
