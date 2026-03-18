#!/bin/bash
#source energy_env/bin/activate
mkdir -p logs

python3 -c "
import asyncio
import time
import json
import statistics
from broker.redis_client import RedisClient
from broker.queue_manager import QueueManager, Priority

async def run_benchmark():
    r = RedisClient()
    await r.connect()
    client = await r.get_client()
    q = QueueManager(r)

    print('=' * 50)
    print('ENERGYQUEUE BENCHMARK')
    print('=' * 50)

    # Reset counters
    await client.delete('jobs:completed')
    await client.delete('energy:budget:consumed')
    await client.delete('queue:high')
    await client.delete('queue:medium')
    await client.delete('queue:low')

    NUM_JOBS = 50
    print(f'Submitting {NUM_JOBS} jobs...')
    start_time = time.perf_counter()

    for i in range(NUM_JOBS):
        if i % 3 == 0:
            await q.enqueue(job_type='ml_training', payload={'epochs': 1, 'batch_size': 64}, priority=Priority.HIGH)
        elif i % 3 == 1:
            await q.enqueue(job_type='image_resize', payload={'width': 800, 'height': 600, 'output_path': f'/tmp/resized_{i}.jpg'}, priority=Priority.MEDIUM)
        else:
            await q.enqueue(job_type='image_resize', payload={'width': 1920, 'height': 1080, 'output_path': f'/tmp/resized_large_{i}.jpg'}, priority=Priority.LOW)

    submit_time = time.perf_counter() - start_time
    print(f'All {NUM_JOBS} jobs submitted in {submit_time:.3f}s')
    print(f'Submission rate: {NUM_JOBS/submit_time:.1f} jobs/second')
    print()
    print('Waiting for workers to process all jobs...')
    print('(Make sure ./scripts/start_workers.sh is running in another tab)')
    print()

    wait_start = time.perf_counter()
    timeout = 300
    last_count = 0
    job_completion_times = []
    last_check_time = time.perf_counter()

    while True:
        completed = await client.llen('jobs:completed')
        remaining_high = await client.llen('queue:high')
        remaining_med = await client.llen('queue:medium')
        remaining_low = await client.llen('queue:low')
        total_remaining = remaining_high + remaining_med + remaining_low

        now = time.perf_counter()
        if completed != last_count:
            elapsed = now - wait_start
            jobs_done_since_last = completed - last_count
            time_since_last = now - last_check_time
            for _ in range(jobs_done_since_last):
                job_completion_times.append(time_since_last / jobs_done_since_last)
            print(f'  Completed: {completed}/{NUM_JOBS} | Remaining: {total_remaining} | Elapsed: {elapsed:.1f}s')
            last_count = completed
            last_check_time = now

        if completed >= NUM_JOBS or total_remaining == 0:
            break

        if time.perf_counter() - wait_start > timeout:
            print('Timeout waiting for jobs')
            break

        await asyncio.sleep(1)

    total_time = time.perf_counter() - wait_start
    completed = await client.llen('jobs:completed')
    co2 = await client.get('energy:budget:consumed')
    carbon = await client.get('cache:carbon_intensity:GB')

    co2_val = float(co2) if co2 else 0.0
    carbon_val = float(carbon) if carbon else 0.0
    throughput = completed / total_time if total_time > 0 else 0
    avg_time = total_time / completed if completed > 0 else 0

    # Calculate latency percentiles
    if job_completion_times:
        sorted_times = sorted(job_completion_times)
        p50 = statistics.median(sorted_times)
        p95_idx = int(len(sorted_times) * 0.95)
        p99_idx = int(len(sorted_times) * 0.99)
        p95 = sorted_times[min(p95_idx, len(sorted_times)-1)]
        p99 = sorted_times[min(p99_idx, len(sorted_times)-1)]
    else:
        p50 = p95 = p99 = avg_time

    print()
    print('=' * 50)
    print('BENCHMARK RESULTS')
    print('=' * 50)
    print(f'Jobs completed:          {completed}/{NUM_JOBS}')
    print(f'Total time:              {total_time:.2f}s')
    print(f'Throughput:              {throughput:.2f} jobs/second')
    print(f'Avg time per job:        {avg_time:.2f}s')
    print(f'p50 latency:             {p50:.2f}s')
    print(f'p95 latency:             {p95:.2f}s')
    print(f'p99 latency:             {p99:.2f}s')
    print(f'CO2 consumed:            {co2_val:.4f}g')
    print(f'CO2 per job:             {co2_val/completed:.4f}g' if completed > 0 else 'N/A')
    print(f'Grid carbon intensity:   {carbon_val:.1f} gCO2/kWh')
    print(f'Workers used:            3')
    print()

    results = {
        'jobs_submitted': NUM_JOBS,
        'jobs_completed': completed,
        'total_time_seconds': round(total_time, 2),
        'throughput_jobs_per_second': round(throughput, 2),
        'avg_time_per_job_seconds': round(avg_time, 2),
        'p50_latency_seconds': round(p50, 2),
        'p95_latency_seconds': round(p95, 2),
        'p99_latency_seconds': round(p99, 2),
        'co2_consumed_grams': round(co2_val, 4),
        'co2_per_job_grams': round(co2_val/completed, 4) if completed > 0 else 0,
        'grid_carbon_intensity_gco2_kwh': round(carbon_val, 1),
        'workers': 3,
    }

    with open('logs/benchmark_results.txt', 'w') as f:
        f.write('ENERGYQUEUE BENCHMARK RESULTS\n')
        f.write('=' * 40 + '\n')
        for k, v in results.items():
            f.write(f'{k}: {v}\n')

    print('Results saved to logs/benchmark_results.txt')
    print()
    print('RESUME METRICS (copy these):')
    print(f'  - Processed {completed} jobs across 3 concurrent async workers')
    print(f'  - {throughput:.2f} jobs/second end-to-end throughput')
    print(f'  - p50: {p50:.2f}s | p95: {p95:.2f}s | p99: {p99:.2f}s latency')
    print(f'  - {co2_val:.4f}g CO2 tracked via CodeCarbon')
    print(f'  - {NUM_JOBS/submit_time:.0f} jobs/second Redis enqueue rate')
    print(f'  - Live carbon scheduling at {carbon_val:.0f} gCO2/kWh grid intensity')

    await r.disconnect()

asyncio.run(run_benchmark())
" 2>&1 | tee logs/benchmark_output.txt
