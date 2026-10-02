import asyncio
import logging
import statistics
import time
import argparse
from typing import List

from raftkv.client import RaftKVClient

logging.basicConfig(level=logging.WARNING)

async def run_benchmark(cluster_addrs: List[str], num_requests: int = 200, concurrency: int = 10):
    client = RaftKVClient(cluster_addrs)
    
    print("==================================================")
    print("      RaftKV Cluster Benchmark Suite")
    print(f" Cluster Addrs: {cluster_addrs}")
    print(f" Total Requests: {num_requests} | Concurrency: {concurrency}")
    print("==================================================\n")

    # --- Benchmark PUT Performance ---
    print(f"--> Running {num_requests} PUT operations...")
    put_latencies: List[float] = []
    
    sem = asyncio.Semaphore(concurrency)

    async def single_put(i: int):
        async with sem:
            start_t = time.monotonic()
            key = f"bench:key:{i}"
            val = f"bench:val:{i}"
            ok = await client.put(key, val, timeout=5.0)
            elapsed_ms = (time.monotonic() - start_t) * 1000.0
            if ok:
                put_latencies.append(elapsed_ms)

    start_bench = time.monotonic()
    tasks = [single_put(i) for i in range(num_requests)]
    await asyncio.gather(*tasks)
    total_put_time = time.monotonic() - start_bench

    # --- Benchmark GET Performance ---
    print(f"--> Running {num_requests} GET operations...")
    get_latencies: List[float] = []

    async def single_get(i: int):
        async with sem:
            start_t = time.monotonic()
            key = f"bench:key:{i}"
            found, val = await client.get(key, timeout=5.0)
            elapsed_ms = (time.monotonic() - start_t) * 1000.0
            if found:
                get_latencies.append(elapsed_ms)

    start_bench_get = time.monotonic()
    tasks_get = [single_get(i) for i in range(num_requests)]
    await asyncio.gather(*tasks_get)
    total_get_time = time.monotonic() - start_bench_get

    # --- Results Calculation ---
    def print_metrics(name: str, latencies: List[float], duration_sec: float):
        if not latencies:
            print(f"No successful {name} operations recorded.")
            return
        sorted_lat = sorted(latencies)
        count = len(sorted_lat)
        qps = count / duration_sec
        avg_lat = statistics.mean(sorted_lat)
        p50 = sorted_lat[int(count * 0.50)]
        p95 = sorted_lat[min(int(count * 0.95), count - 1)]
        p99 = sorted_lat[min(int(count * 0.99), count - 1)]

        print(f"--- {name} Results ---")
        print(f" Successful Ops : {count} / {num_requests}")
        print(f" Total Duration : {duration_sec:.2f} s")
        print(f" Throughput     : {qps:.2f} ops/sec (QPS)")
        print(f" Avg Latency    : {avg_lat:.2f} ms")
        print(f" p50 Latency    : {p50:.2f} ms")
        print(f" p95 Latency    : {p95:.2f} ms")
        print(f" p99 Latency    : {p99:.2f} ms\n")

    print_metrics("PUT", put_latencies, total_put_time)
    print_metrics("GET", get_latencies, total_get_time)

def main():
    parser = argparse.ArgumentParser(description="RaftKV Benchmark Tool")
    parser.add_argument("--cluster", default="127.0.0.1:50201,127.0.0.1:50202,127.0.0.1:50203,127.0.0.1:50204,127.0.0.1:50205")
    parser.add_argument("--num-requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    args = parser.parse_args()

    addrs = args.cluster.split(",")
    asyncio.run(run_benchmark(addrs, args.num_requests, args.concurrency))

if __name__ == "__main__":
    main()
