import asyncio
import logging
from typing import Dict, Optional
import grpc

from raftkv.proto import raft_pb2, raft_pb2_grpc
from raftkv.consensus.engine import RaftEngine

logger = logging.getLogger(__name__)

class RaftConsensusService(raft_pb2_grpc.RaftConsensusServicer):
    """
    gRPC Servicer for Raft internal consensus RPCs.
    """
    def __init__(self, engine: RaftEngine):
        self.engine = engine

    async def RequestVote(
        self, request: raft_pb2.RequestVoteArgs, context: grpc.aio.ServicerContext
    ) -> raft_pb2.RequestVoteReply:
        return await self.engine.handle_request_vote(request)

    async def AppendEntries(
        self, request: raft_pb2.AppendEntriesArgs, context: grpc.aio.ServicerContext
    ) -> raft_pb2.AppendEntriesReply:
        return await self.engine.handle_append_entries(request)

    async def InstallSnapshot(
        self, request: raft_pb2.SnapshotArgs, context: grpc.aio.ServicerContext
    ) -> raft_pb2.SnapshotReply:
        return await self.engine.handle_install_snapshot(request)


class ClientService(raft_pb2_grpc.ClientServiceServicer):
    """
    gRPC Servicer for external Client KV operations (Put, Get, Delete)
    with automatic leader redirection hints.
    """
    def __init__(self, engine: RaftEngine):
        self.engine = engine

    async def Put(
        self, request: raft_pb2.PutArgs, context: grpc.aio.ServicerContext
    ) -> raft_pb2.PutReply:
        success, val, err, leader_id, leader_addr = await self.engine.execute_client_command(
            op=raft_pb2.OpType.PUT,
            key=request.key,
            value=request.value,
            client_id=request.client_id,
            seq=request.sequence_num
        )
        return raft_pb2.PutReply(
            success=success,
            error=err or "",
            leader_id=leader_id or "",
            leader_address=leader_addr or ""
        )

    async def Get(
        self, request: raft_pb2.GetArgs, context: grpc.aio.ServicerContext
    ) -> raft_pb2.GetReply:
        success, val, err, leader_id, leader_addr = await self.engine.read_client_command(
            key=request.key
        )
        found = (err is None and val is not None)
        return raft_pb2.GetReply(
            success=success,
            value=val or "",
            found=found,
            error=err or "",
            leader_id=leader_id or "",
            leader_address=leader_addr or ""
        )

    async def Delete(
        self, request: raft_pb2.DeleteArgs, context: grpc.aio.ServicerContext
    ) -> raft_pb2.DeleteReply:
        success, val, err, leader_id, leader_addr = await self.engine.execute_client_command(
            op=raft_pb2.OpType.DELETE,
            key=request.key,
            value="",
            client_id=request.client_id,
            seq=request.sequence_num
        )
        return raft_pb2.DeleteReply(
            success=success,
            error=err or "",
            leader_id=leader_id or "",
            leader_address=leader_addr or ""
        )


async def serve_node(node_id: str, peer_addresses: Dict[str, str], port: int, data_dir: str):
    """
    Starts the RaftKV Async gRPC server and Consensus Engine.
    """
    engine = RaftEngine(node_id=node_id, peer_addresses=peer_addresses, data_dir=data_dir)
    await engine.start()

    server = grpc.aio.server()
    raft_pb2_grpc.add_RaftConsensusServicer_to_server(RaftConsensusService(engine), server)
    raft_pb2_grpc.add_ClientServiceServicer_to_server(ClientService(engine), server)

    listen_addr = f"[::]:{port}"
    server.add_insecure_port(listen_addr)
    logger.info(f"[{node_id}] gRPC Server running on {listen_addr}")
    
    await server.start()
    
    try:
        await server.wait_for_termination()
    finally:
        await engine.stop()
        await server.stop(0)
