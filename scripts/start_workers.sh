#!/bin/bash
source energy_env/bin/activate
mkdir -p logs
python3 -c "
import asyncio, logging, os
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('logs/worker.log'),
        logging.StreamHandler()
    ]
)
from broker.redis_client import RedisClient
from broker.queue_manager import QueueManager
from worker.worker_pool import WorkerPool

async def main():
    r = RedisClient()
    q = QueueManager(r)
    pool = WorkerPool(queue=q, redis=r, size=5)
    await pool.start()

asyncio.run(main())
"
