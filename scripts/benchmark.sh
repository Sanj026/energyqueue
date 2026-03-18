#!/bin/bash
# EnergyQueue Benchmark Script
# Run this with workers already running in another tab
# Results saved to logs/benchmark_results.txt

source energy_env/bin/activate
mkdir -p logs

python3 -c "
import asyncio
import time
import json
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

    # Submit 50 jobs
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
    print(f'Throughput: {NUM_JOBS/submit_time:.1f} jobs/second (submission rate)')
    print()
    print('Waiting for workers to process all jobs...')
    print('(Make sure ./scripts/start_workers.sh is running in another tab)')
    print()

    # Wait for completion
    wait_start = time.perf_counter()
    timeout = 300  # 5 minutes max
    last_count = 0

    while True:
        completed = await client.llen('jobs:completed')
        remaining_high = await client.llen('queue:high')
        remaining_med = await client.llen('queue:medium')
        remaining_low = await client.llen('queue:low')
        total_remaining = remaining_high + remaining_med + remaining_low

        if completed != last_count:
            elapsed = time.perf_counter() - wait_start
            print(f'  Completed: {completed}/{NUM_JOBS} | Remaining: {total_remaining} | Elapsed: {elapsed:.1f}s')
            last_count = completed

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

    # Calculate metrics
    throughput = completed / total_time if total_time > 0 else 0
    avg_time = total_time / completed if completed > 0 else 0
    co2_val = float(co2) if co2 else 0.0
    carbon_val = float(carbon) if carbon else 0.0

    print()
    print('=' * 50)
    print('BENCHMARK RESULTS')
    print('=' * 50)
    print(f'Jobs completed:          {completed}/{NUM_JOBS}')
    print(f'Total time:              {total_time:.2f}s')
    print(f'Throughput:              {throughput:.2f} jobs/second')
    print(f'Avg time per job:        {avg_time:.2f}s')
    print(f'CO2 consumed:            {co2_val:.4f}g')
    print(f'CO2 per job:             {co2_val/completed:.4f}g' if completed > 0 else 'N/A')
    print(f'Grid carbon intensity:   {carbon_val:.1f} gCO2/kWh')
    print(f'Workers used:            3')
    print()

    # Save to file
    results = {
        'jobs_submitted': NUM_JOBS,
        'jobs_completed': completed,
        'total_time_seconds': round(total_time, 2),
        'throughput_jobs_per_second': round(throughput, 2),
        'avg_time_per_job_seconds': round(avg_time, 2),
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
    print(f'  - Processed {completed} jobs across 3 concurrent workers')
    print(f'  - {throughput:.2f} jobs/second throughput')
    print(f'  - {avg_time:.2f}s average job execution time')
    print(f'  - {co2_val:.4f}g CO2 tracked across all jobs')
    print(f'  - Real-time carbon scheduling at {carbon_val:.0f} gCO2/kWh grid intensity')

    await r.disconnect()

asyncio.run(run_benchmark())
" 2>&1 | tee logs/benchmark_output.txt
