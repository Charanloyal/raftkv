# Product Requirements Document (PRD)
## RaftKV: Distributed Strongly-Consistent Key-Value Store

| Document Version | 1.0.0 |
| :--- | :--- |
| Status | Approved & Production Implemented |
| Author | Charan Loyal (Distributed Systems & AI Infrastructure Engineer) |
| Target System | 5-Node High-Availability Replicated Key-Value Consensus Store |
| Core Protocols | Raft Consensus (Ongaro & Ousterhout), gRPC over HTTP/2, Protobuf v3 |

---

## 1. Executive Summary & Vision

Modern cloud-native and quant infrastructure architectures require zero-data-loss, linearizable state storage to coordinate critical microservices, distributed locks, configuration flags, and transactional ledgers. **RaftKV** is a clean-slate, production-grade, distributed key-value store engineered in Python 3.11+ using the **Raft Consensus Algorithm** running over asynchronous gRPC stubs.

The system guarantees **Linearizability (Strong Consistency)**, **High Availability under $f < N/2$ node failures**, **ACID Write-Ahead Logging (WAL)** via an embedded engine, **dynamic log compaction with snapshots**, and automated **smart-client leader redirection**.

---

## 2. Key Goals & Success Metrics

| Goal ID | Objective | Target SLA / Metric | Verification Method |
| :--- | :--- | :--- | :--- |
| **G-01** | Sub-Second Leader Failover | New leader elected in **< 1.0s** after crash | Automated Chaos Harness |
| **G-02** | Fault Tolerance ($N=5$) | Survive **up to 2 simultaneous node crashes** with zero data loss | 5-node Docker & In-Memory Cluster Harness |
| **G-03** | Linearizable Consistency | 100% linearizable reads and writes; zero dirty or phantom reads | Linearizability verification suite |
| **G-04** | High Read/Write Throughput | **> 1,500 GET ops/sec**, **> 500 PUT ops/sec** under concurrency | Asynchronous Locust & Benchmark script |
| **G-05** | Memory Boundedness | Automatic log compaction & snapshotting at 50 log entries | WAL & Snapshot storage unit tests |

---

## 3. System Personas & User Scenarios

1. **Distributed Infrastructure Engineer**:
   - Deploys a 5-node RaftKV cluster across availability zones or isolated Docker containers.
   - Requires deterministic health endpoints, Prometheus observability (`/metrics`), and automatic cluster bootstrap.
2. **Quant Trading & Microservices Developer**:
   - Uses `RaftKVClient` to execute low-latency atomic operations (`PUT`, `GET`, `DELETE`).
   - Expects transparent retries, at-most-once semantics via sequence IDs, and automatic redirection when talking to non-leader nodes.
3. **Site Reliability Engineer (SRE)**:
   - Injects chaos faults (node kills, SIGKILL, network partitions).
   - Monitors cluster state, term changes, and log replication lag via the interactive Web Control Plane dashboard (`web_demo.py`).

---

## 4. Functional Requirements

### 4.1 Raft Consensus Core
- **Role Transitions**: Deterministic state transitions across `FOLLOWER`, `CANDIDATE`, and `LEADER`.
- **Randomized Election Timers**: Randomized timeouts between $150\text{ ms} - 350\text{ ms}$ to eliminate split-vote deadlocks.
- **Heartbeat & Log Replication**: Periodic `AppendEntries` heartbeats sent every $50\text{ ms}$ from the active leader.
- **Log Matching Property**: If two entries in different logs have the same index and term, they store the same command; logs are identical up to that index.
- **Log Up-To-Date Verification**: Candidates must prove their log is at least as up-to-date as the voter's log before receiving a vote (`lastLogTerm` and `lastLogIndex` check).
- **Commit Index Progression**: Leader commits an entry when a strict majority ($Q = \lfloor N/2 \rfloor + 1 = 3$ in a 5-node cluster) acknowledges replication.

### 4.2 Durable Storage & WAL
- **Write-Ahead Logging Engine**: ACID SQLite-backed log storing persistent state (`currentTerm`, `votedFor`, log entries).
- **Log Compaction**: Automatic snapshot generation when uncompacted logs cross a configurable threshold (default 50 entries).
- **State Catch-up via `InstallSnapshot`**: Sluggish or newly recovered nodes receive full state machine snapshots when their `nextIndex` falls behind the leader's compacted log range.

### 4.3 Smart Client Protocol
- **Leader Discovery**: Clients query any node; followers reply with `NOT_LEADER` and an advisory `leader_hint` (IP:Port).
- **Request Deduplication**: Each client session maintains a unique `client_id` and monotonically increasing `sequence_num` to ensure strictly at-most-once execution on retries.
- **Exponential Backoff**: Jittered exponential backoff when re-establishing communication during leader election windows.

### 4.4 Fault-Injection & Chaos Testing Suite
- **Chaos Scenarios**:
  1. *Leader Failure*: Immediate SIGTERM/SIGKILL of the leader; assert election of successor in $< 1.0\text{s}$.
  2. *Minority Partition ($N=5 \rightarrow 3 + 2$)*: Nodes 4 and 5 isolated; majority partition ($1, 2, 3$) continues committing writes.
  3. *Reconnection & Log Truncation*: Reconnected minority synchronizes state from leader without state corruption.
  4. *High-Concurrency Churn*: Concurrent background write streams during rolling restarts with zero lost updates.

---

## 5. Non-Functional Requirements

- **Language & Runtime**: Python 3.11+ using `asyncio` and `grpc.aio`.
- **Communication Protocol**: gRPC over HTTP/2, Protobuf serialization for sub-millisecond wire overhead.
- **Portability**: Containerized deployment via multi-stage Dockerfile and Docker Compose 5-node cluster.
- **Observability**: Prometheus metrics export (`ops_total`, `latency_p95`, `election_count`) and real-time Web Dashboard.
- **Code Quality**: 100% test pass rate across unit, WAL persistence, and chaos integration suites.
