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
    for i in range(8):
        await q.enqueue(
            job_type='ml_training',
            payload={'epochs': 2, 'batch_size': 32},
            priority=Priority.HIGH,
        )
    for i in range(8):
        await q.enqueue(
            job_type='ml_training',
            payload={'epochs': 1, 'batch_size': 64},
            priority=Priority.MEDIUM,
        )
    for i in range(4):
        await q.enqueue(
            job_type='image_resize',
            payload={'width': 1920, 'height': 1080, 'output_path': f'/tmp/resized_demo_{i}.jpg'},
            priority=Priority.LOW,
        )
    print('20 jobs submitted!')
    await r.disconnect()

asyncio.run(main())
"
