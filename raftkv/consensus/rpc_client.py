import asyncio
import logging
from typing import Dict, Optional
import grpc
from raftkv.proto import raft_pb2, raft_pb2_grpc

logger = logging.getLogger(__name__)

class PeerRPCClient:
    """
    Asynchronous gRPC Client Manager for node-to-node Raft RPCs.
    Maintains persistent gRPC channels and handles network timeouts/failures gracefully.
    """
    def __init__(self, peer_addresses: Dict[str, str]):
        # peer_id -> address string (e.g. "raft-node-2:50051")
        self.peer_addresses = peer_addresses
        self.channels: Dict[str, grpc.aio.Channel] = {}
        self.stubs: Dict[str, raft_pb2_grpc.RaftConsensusStub] = {}

    def _get_stub(self, peer_id: str) -> Optional[raft_pb2_grpc.RaftConsensusStub]:
        addr = self.peer_addresses.get(peer_id)
        if not addr:
            return None
        if peer_id not in self.stubs:
            channel = grpc.aio.insecure_channel(addr)
            self.channels[peer_id] = channel
            self.stubs[peer_id] = raft_pb2_grpc.RaftConsensusStub(channel)
        return self.stubs[peer_id]

    async def send_request_vote(
        self, peer_id: str, args: raft_pb2.RequestVoteArgs, timeout: float = 0.15
    ) -> Optional[raft_pb2.RequestVoteReply]:
        stub = self._get_stub(peer_id)
        if not stub:
            return None
        try:
            reply = await stub.RequestVote(args, timeout=timeout)
            return reply
        except grpc.RpcError as e:
            logger.debug(f"[RPCClient] RequestVote to {peer_id} failed: {e.code()}")
            return None
        except Exception as e:
            logger.debug(f"[RPCClient] Exception in RequestVote to {peer_id}: {e}")
            return None

    async def send_append_entries(
        self, peer_id: str, args: raft_pb2.AppendEntriesArgs, timeout: float = 0.15
    ) -> Optional[raft_pb2.AppendEntriesReply]:
        stub = self._get_stub(peer_id)
        if not stub:
            return None
        try:
            reply = await stub.AppendEntries(args, timeout=timeout)
            return reply
        except grpc.RpcError as e:
            logger.debug(f"[RPCClient] AppendEntries to {peer_id} failed: {e.code()}")
            return None
        except Exception as e:
            logger.debug(f"[RPCClient] Exception in AppendEntries to {peer_id}: {e}")
            return None

    async def send_install_snapshot(
        self, peer_id: str, args: raft_pb2.SnapshotArgs, timeout: float = 0.50
    ) -> Optional[raft_pb2.SnapshotReply]:
        stub = self._get_stub(peer_id)
        if not stub:
            return None
        try:
            reply = await stub.InstallSnapshot(args, timeout=timeout)
            return reply
        except grpc.RpcError as e:
            logger.debug(f"[RPCClient] InstallSnapshot to {peer_id} failed: {e.code()}")
            return None
        except Exception as e:
            logger.debug(f"[RPCClient] Exception in InstallSnapshot to {peer_id}: {e}")
            return None

    async def close(self):
        for channel in self.channels.values():
            await channel.close()
        self.channels.clear()
        self.stubs.clear()
