#!/bin/bash
source energy_env/bin/activate
python3 -c "
import asyncio
from broker.redis_client import RedisClient
from broker.queue_manager import QueueManager, Priority

async def main():
    r = RedisClient()
    await r.connect()
    q = QueueManager(r)
    await q.enqueue(job_type='ml_training', payload={'epochs': 2, 'batch_size': 32}, priority=Priority.HIGH)
    await q.enqueue(job_type='ml_training', payload={'epochs': 1, 'batch_size': 64}, priority=Priority.MEDIUM)
    await q.enqueue(job_type='image_resize', payload={'width': 800, 'height': 600, 'output_path': '/tmp/resized_1.jpg'}, priority=Priority.HIGH)
    await q.enqueue(job_type='image_resize', payload={'width': 1920, 'height': 1080, 'output_path': '/tmp/resized_2.jpg'}, priority=Priority.LOW)
    print('4 jobs submitted!')
    await r.disconnect()

asyncio.run(main())
"
