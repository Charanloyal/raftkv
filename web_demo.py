import asyncio
import json
import logging
import os
import shutil
import tempfile
import time
from typing import Dict, List, Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse
import threading

import grpc
from raftkv.server import serve_node
from raftkv.client import RaftKVClient
from raftkv.proto import raft_pb2, raft_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("RaftKVWebDemo")

class DemoClusterManager:
    def __init__(self, node_count: int = 5, base_port: int = 50400):
        self.node_count = node_count
        self.base_port = base_port
        self.temp_dir = tempfile.mkdtemp(prefix="raftkv_web_")
        self.nodes_meta: Dict[str, dict] = {}
        self.tasks: Dict[str, asyncio.Task] = {}
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.ops_counter = 0
        self.latencies = []
        
        self.peer_addresses = {
            f"node-{i}": f"127.0.0.1:{base_port + i}"
            for i in range(1, node_count + 1)
        }

    def start_in_loop(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        asyncio.run_coroutine_threadsafe(self._async_start(), loop)

    async def _async_start(self):
        for node_id, addr in self.peer_addresses.items():
            port = int(addr.split(":")[1])
            node_dir = os.path.join(self.temp_dir, node_id)
            os.makedirs(node_dir, exist_ok=True)
            self.nodes_meta[node_id] = {"port": port, "dir": node_dir, "status": "running"}
            
            task = asyncio.create_task(
                serve_node(
                    node_id=node_id,
                    peer_addresses=dict(self.peer_addresses),
                    port=port,
                    data_dir=node_dir
                )
            )
            self.tasks[node_id] = task
        logger.info("Demo cluster launched.")

    async def get_cluster_status(self) -> dict:
        status_data = []
        leader_id = ""

        for node_id, addr in self.peer_addresses.items():
            is_running = (node_id in self.tasks and not self.tasks[node_id].done())
            node_info = {
                "node_id": node_id,
                "address": addr,
                "status": "ONLINE" if is_running else "OFFLINE",
                "role": "OFFLINE",
                "term": 0,
                "commit_index": 0,
                "log_len": 0,
                "leader_id": ""
            }

            if is_running:
                try:
                    async with grpc.aio.insecure_channel(addr) as channel:
                        stub = raft_pb2_grpc.ClientServiceStub(channel)
                        reply = await stub.Get(raft_pb2.GetArgs(key="__leader_check__"), timeout=0.3)
                        if reply.leader_id:
                            node_info["leader_id"] = reply.leader_id
                            leader_id = reply.leader_id
                            node_info["role"] = "LEADER" if reply.leader_id == node_id else "FOLLOWER"
                except Exception:
                    pass

            status_data.append(node_info)

        return {
            "nodes": status_data,
            "leader_id": leader_id,
            "peer_addresses": self.peer_addresses,
            "ops_total": self.ops_counter,
            "avg_latency_ms": round(sum(self.latencies[-20:]) / max(1, len(self.latencies[-20:])), 2)
        }

    async def stop_node(self, node_id: str):
        if node_id in self.tasks and not self.tasks[node_id].done():
            self.tasks[node_id].cancel()
            self.nodes_meta[node_id]["status"] = "stopped"
            logger.info(f"Node {node_id} stopped via Web UI.")

    async def restart_node(self, node_id: str):
        if node_id in self.nodes_meta and (node_id not in self.tasks or self.tasks[node_id].done()):
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
            meta["status"] = "running"
            logger.info(f"Node {node_id} restarted via Web UI.")

CLUSTER = DemoClusterManager()
EVENT_LOOP: Optional[asyncio.AbstractEventLoop] = None

HTML_DASHBOARD = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RaftKV — Distributed Consensus Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=Fira+Code:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0b0f19;
            --bg-card: rgba(22, 30, 49, 0.7);
            --border: rgba(255, 255, 255, 0.08);
            --accent-cyan: #00f2fe;
            --accent-blue: #4facfe;
            --accent-green: #10b981;
            --accent-red: #ef4444;
            --accent-amber: #f59e0b;
            --text-main: #f3f4f6;
            --text-muted: #9ca3af;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Outfit', sans-serif;
            background: var(--bg-primary);
            color: var(--text-main);
            min-height: 100vh;
            padding: 2rem;
            background-image: 
                radial-gradient(circle at 10% 20%, rgba(0, 242, 254, 0.05) 0%, transparent 40%),
                radial-gradient(circle at 90% 80%, rgba(79, 172, 254, 0.05) 0%, transparent 40%);
        }

        .container { max-width: 1200px; margin: 0 auto; }
        
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            padding-bottom: 1rem;
            border-bottom: 1px solid var(--border);
        }

        .logo-title { display: flex; align-items: center; gap: 0.75rem; }
        .logo-icon {
            width: 44px; height: 44px;
            background: linear-gradient(135deg, var(--accent-cyan), var(--accent-blue));
            border-radius: 12px;
            display: grid; place-items: center;
            font-weight: 700; color: #000; font-size: 1.2rem;
        }

        h1 { font-size: 1.75rem; font-weight: 700; letter-spacing: -0.5px; }
        .subtitle { color: var(--text-muted); font-size: 0.9rem; }

        .metrics-bar {
            display: flex; gap: 1rem; margin-bottom: 1.5rem;
        }
        .metric-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 0.75rem 1.25rem;
            flex: 1;
            display: flex; flex-direction: column;
        }
        .metric-title { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; font-weight: 600; }
        .metric-value { font-size: 1.4rem; font-weight: 700; font-family: 'Fira Code', monospace; color: var(--accent-cyan); }

        .badge {
            background: rgba(16, 185, 129, 0.15);
            color: var(--accent-green);
            padding: 0.4rem 0.8rem;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: 600;
            border: 1px solid rgba(16, 185, 129, 0.3);
            display: flex; align-items: center; gap: 0.5rem;
        }

        .dot { width: 8px; height: 8px; background: var(--accent-green); border-radius: 50%; animation: pulse 1.5s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }

        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 1.25rem; margin-bottom: 2rem; }

        .card {
            background: var(--bg-card);
            backdrop-filter: blur(12px);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1.25rem;
            transition: all 0.3s ease;
        }
        .card:hover { transform: translateY(-2px); border-color: rgba(255, 255, 255, 0.15); }

        .node-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; }
        .node-id { font-weight: 600; font-size: 1.1rem; }

        .role-pill {
            padding: 0.25rem 0.6rem;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 700;
            text-transform: uppercase;
        }
        .role-leader { background: rgba(0, 242, 254, 0.2); color: var(--accent-cyan); border: 1px solid var(--accent-cyan); }
        .role-follower { background: rgba(156, 163, 175, 0.15); color: var(--text-muted); border: 1px solid var(--border); }
        .role-offline { background: rgba(239, 68, 68, 0.2); color: var(--accent-red); border: 1px solid var(--accent-red); }

        .stat-row { display: flex; justify-content: space-between; margin-bottom: 0.5rem; font-size: 0.85rem; }
        .stat-label { color: var(--text-muted); }
        .stat-val { font-family: 'Fira Code', monospace; font-weight: 500; }

        .actions { display: flex; gap: 0.5rem; margin-top: 1rem; }
        .btn {
            flex: 1;
            padding: 0.5rem;
            border: none;
            border-radius: 8px;
            font-family: inherit;
            font-size: 0.8rem;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.2s;
        }
        .btn-kill { background: rgba(239, 68, 68, 0.2); color: var(--accent-red); border: 1px solid rgba(239, 68, 68, 0.4); }
        .btn-kill:hover { background: var(--accent-red); color: #fff; }
        .btn-start { background: rgba(16, 185, 129, 0.2); color: var(--accent-green); border: 1px solid rgba(16, 185, 129, 0.4); }
        .btn-start:hover { background: var(--accent-green); color: #fff; }

        .panel {
            background: var(--bg-card);
            backdrop-filter: blur(12px);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1.5rem;
        }
        .panel-title { font-size: 1.1rem; font-weight: 600; margin-bottom: 1rem; }

        .form-row { display: flex; gap: 1rem; margin-bottom: 1rem; }
        input, select {
            background: rgba(0, 0, 0, 0.3);
            border: 1px solid var(--border);
            color: #fff;
            padding: 0.75rem 1rem;
            border-radius: 10px;
            font-family: inherit;
            outline: none;
        }
        input:focus { border-color: var(--accent-cyan); }

        .btn-primary {
            background: linear-gradient(135deg, var(--accent-cyan), var(--accent-blue));
            color: #000;
            font-weight: 700;
            padding: 0.75rem 1.5rem;
            border-radius: 10px;
            border: none;
            cursor: pointer;
        }

        .console-output {
            background: #05070d;
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 1rem;
            font-family: 'Fira Code', monospace;
            font-size: 0.85rem;
            height: 180px;
            overflow-y: auto;
            color: #a7f3d0;
        }
        .log-entry { margin-bottom: 0.4rem; }
        .log-err { color: #fca5a5; }

        .link-bar {
            margin-top: 1rem; display: flex; justify-content: space-between; font-size: 0.85rem; color: var(--text-muted);
        }
        .link-bar a { color: var(--accent-cyan); text-decoration: none; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="logo-title">
                <div class="logo-icon">R</div>
                <div>
                    <h1>RaftKV Distributed Control Plane</h1>
                    <div class="subtitle">Strongly Consistent Raft Consensus Engine & Observability Suite</div>
                </div>
            </div>
            <div class="badge">
                <div class="dot"></div>
                <span>5-Node Cluster Active</span>
            </div>
        </header>

        <div class="metrics-bar">
            <div class="metric-card">
                <span class="metric-title">Cluster Leader</span>
                <span class="metric-value" id="m-leader">Searching...</span>
            </div>
            <div class="metric-card">
                <span class="metric-title">Total Operations</span>
                <span class="metric-value" id="m-ops">0</span>
            </div>
            <div class="metric-card">
                <span class="metric-title">Avg Latency</span>
                <span class="metric-value" id="m-lat">0.00 ms</span>
            </div>
            <div class="metric-card">
                <span class="metric-title">Prometheus Metrics</span>
                <span class="metric-value" style="font-size: 1rem; margin-top: 0.3rem;"><a href="/metrics" target="_blank" style="color: var(--accent-cyan);">/metrics</a></span>
            </div>
        </div>

        <h2 style="font-size: 1.1rem; margin-bottom: 1rem; color: var(--text-muted);">CLUSTER TOPOLOGY & CHAOS INJECTION</h2>
        <div class="grid" id="nodes-grid"></div>

        <div class="panel">
            <div class="panel-title">⚡ Interactive Client KV Console & Deduplication Engine</div>
            <div class="form-row">
                <select id="op-select">
                    <option value="PUT">PUT</option>
                    <option value="GET">GET</option>
                    <option value="DELETE">DELETE</option>
                </select>
                <input type="text" id="key-input" placeholder="Key (e.g. user:100)" style="flex: 1;">
                <input type="text" id="val-input" placeholder="Value (e.g. Alice)" style="flex: 1;">
                <button class="btn-primary" onclick="executeOp()">Execute RPC</button>
            </div>

            <div class="console-output" id="console">
                <div class="log-entry">[System] RaftKV Interactive Client Ready. Prom Metrics available at /metrics.</div>
            </div>
        </div>

        <div class="link-bar">
            <span>RaftKV Architecture • SQLite WAL Engine • Protobuf over gRPC</span>
            <a href="https://github.com/Charanloyal/raftkv" target="_blank">View GitHub Repository ↗</a>
        </div>
    </div>

    <script>
        async function fetchStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                renderNodes(data.nodes);
                document.getElementById('m-leader').textContent = data.leader_id || 'Electing...';
                document.getElementById('m-ops').textContent = data.ops_total;
                document.getElementById('m-lat').textContent = data.avg_latency_ms + ' ms';
            } catch (e) {
                console.error("Status fetch error", e);
            }
        }

        function renderNodes(nodes) {
            const grid = document.getElementById('nodes-grid');
            grid.innerHTML = nodes.map(n => `
                <div class="card">
                    <div class="node-header">
                        <span class="node-id">${n.node_id}</span>
                        <span class="role-pill ${n.role === 'LEADER' ? 'role-leader' : (n.role === 'FOLLOWER' ? 'role-follower' : 'role-offline')}">
                            ${n.role}
                        </span>
                    </div>
                    <div class="stat-row"><span class="stat-label">Address</span><span class="stat-val">${n.address}</span></div>
                    <div class="stat-row"><span class="stat-label">Status</span><span class="stat-val">${n.status}</span></div>
                    <div class="stat-row"><span class="stat-label">Known Leader</span><span class="stat-val">${n.leader_id || '-'}</span></div>
                    <div class="actions">
                        ${n.status === 'ONLINE' ? 
                            `<button class="btn btn-kill" onclick="killNode('${n.node_id}')">⚡ Kill Node</button>` :
                            `<button class="btn btn-start" onclick="startNode('${n.node_id}')">▶ Start Node</button>`
                        }
                    </div>
                </div>
            `).join('');
        }

        async function killNode(nodeId) {
            log(`[Chaos] Terminating ${nodeId}...`);
            await fetch('/api/chaos/kill?node_id=' + nodeId, { method: 'POST' });
            setTimeout(fetchStatus, 300);
        }

        async function startNode(nodeId) {
            log(`[Cluster] Restarting ${nodeId}...`);
            await fetch('/api/chaos/start?node_id=' + nodeId, { method: 'POST' });
            setTimeout(fetchStatus, 500);
        }

        async function executeOp() {
            const op = document.getElementById('op-select').value;
            const key = document.getElementById('key-input').value.trim();
            const val = document.getElementById('val-input').value.trim();

            if (!key) { alert('Key is required'); return; }

            log(`[Client] Sending ${op}(key='${key}') ...`);

            try {
                const res = await fetch('/api/op', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ op, key, value: val })
                });
                const data = await res.json();
                if (data.success) {
                    log(`[Success] ${op} result: ${JSON.stringify(data.result)}`);
                } else {
                    log(`[Error] ${op} failed: ${data.error}`, true);
                }
                fetchStatus();
            } catch (e) {
                log(`[Error] Request exception: ${e}`, true);
            }
        }

        function log(msg, isErr = false) {
            const consoleEl = document.getElementById('console');
            const timeStr = new Date().toLocaleTimeString();
            const entry = document.createElement('div');
            entry.className = isErr ? 'log-entry log-err' : 'log-entry';
            entry.textContent = `[${timeStr}] ${msg}`;
            consoleEl.appendChild(entry);
            consoleEl.scrollTop = consoleEl.scrollHeight;
        }

        setInterval(fetchStatus, 1000);
        fetchStatus();
    </script>
</body>
</html>
"""

class WebDemoHandler(BaseHTTPRequestHandler):
    def _set_json_headers(self, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in ["/", "/index.html"]:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(HTML_DASHBOARD.encode("utf-8"))

        elif parsed.path == "/metrics":
            # Expose Prometheus Metrics Endpoint
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            
            future = asyncio.run_coroutine_threadsafe(CLUSTER.get_cluster_status(), EVENT_LOOP)
            status_data = future.result(timeout=2.0)
            
            metrics = [
                "# HELP raft_cluster_nodes_total Total number of nodes configured in Raft cluster",
                "# TYPE raft_cluster_nodes_total gauge",
                f"raft_cluster_nodes_total 5",
                "# HELP raftkv_operations_total Total client operations executed",
                "# TYPE raftkv_operations_total counter",
                f"raftkv_operations_total {CLUSTER.ops_counter}",
                "# HELP raftkv_avg_latency_ms Average operation latency in milliseconds",
                "# TYPE raftkv_avg_latency_ms gauge",
                f"raftkv_avg_latency_ms {round(sum(CLUSTER.latencies[-20:]) / max(1, len(CLUSTER.latencies[-20:])), 2)}"
            ]
            for n in status_data.get("nodes", []):
                is_online = 1 if n["status"] == "ONLINE" else 0
                is_leader = 1 if n["role"] == "LEADER" else 0
                metrics.append(f'raft_node_online{{node_id="{n["node_id"]}"}} {is_online}')
                metrics.append(f'raft_node_is_leader{{node_id="{n["node_id"]}"}} {is_leader}')

            self.wfile.write("\n".join(metrics).encode("utf-8"))

        elif parsed.path == "/api/status":
            future = asyncio.run_coroutine_threadsafe(CLUSTER.get_cluster_status(), EVENT_LOOP)
            status_data = future.result(timeout=2.0)
            self._set_json_headers(200)
            self.wfile.write(json.dumps(status_data).encode("utf-8"))
        else:
            self.send_error(404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(length) if length > 0 else b""
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/api/chaos/kill":
            node_id = params.get("node_id", [""])[0]
            future = asyncio.run_coroutine_threadsafe(CLUSTER.stop_node(node_id), EVENT_LOOP)
            future.result(timeout=2.0)
            self._set_json_headers(200)
            self.wfile.write(json.dumps({"success": True}).encode("utf-8"))

        elif parsed.path == "/api/chaos/start":
            node_id = params.get("node_id", [""])[0]
            future = asyncio.run_coroutine_threadsafe(CLUSTER.restart_node(node_id), EVENT_LOOP)
            future.result(timeout=2.0)
            self._set_json_headers(200)
            self.wfile.write(json.dumps({"success": True}).encode("utf-8"))

        elif parsed.path == "/api/op":
            payload = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
            op = payload.get("op", "GET")
            key = payload.get("key", "")
            val = payload.get("value", "")

            client = RaftKVClient(list(CLUSTER.peer_addresses.values()))
            
            async def run_op():
                start_t = time.monotonic()
                if op == "PUT":
                    res = await client.put(key, val, timeout=3.0)
                    elapsed = (time.monotonic() - start_t) * 1000.0
                    CLUSTER.ops_counter += 1
                    CLUSTER.latencies.append(elapsed)
                    return {"success": res, "result": "OK" if res else "FAILED"}
                elif op == "GET":
                    found, value = await client.get(key, timeout=3.0)
                    elapsed = (time.monotonic() - start_t) * 1000.0
                    CLUSTER.ops_counter += 1
                    CLUSTER.latencies.append(elapsed)
                    return {"success": found, "result": value if found else "NOT_FOUND"}
                elif op == "DELETE":
                    res = await client.delete(key, timeout=3.0)
                    elapsed = (time.monotonic() - start_t) * 1000.0
                    CLUSTER.ops_counter += 1
                    CLUSTER.latencies.append(elapsed)
                    return {"success": res, "result": "DELETED" if res else "FAILED"}
                return {"success": False, "error": "Unknown op"}

            future = asyncio.run_coroutine_threadsafe(run_op(), EVENT_LOOP)
            res = future.result(timeout=4.0)
            self._set_json_headers(200)
            self.wfile.write(json.dumps(res).encode("utf-8"))
        else:
            self.send_error(404)

def run_web_server(port: int = 8080):
    global EVENT_LOOP
    EVENT_LOOP = asyncio.new_event_loop()

    def loop_thread():
        asyncio.set_event_loop(EVENT_LOOP)
        EVENT_LOOP.run_forever()

    t = threading.Thread(target=loop_thread, daemon=True)
    t.start()

    CLUSTER.start_in_loop(EVENT_LOOP)

    server = HTTPServer(("0.0.0.0", port), WebDemoHandler)
    logger.info(f"RaftKV Web Control Plane & Observability Suite running at http://0.0.0.0:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Web Control Plane stopping...")
        server.server_close()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    run_web_server(port)
