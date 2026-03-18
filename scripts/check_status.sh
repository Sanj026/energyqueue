#!/bin/bash
source energy_env/bin/activate
python3 -c "
import asyncio
from broker.redis_client import RedisClient

async def main():
    r = RedisClient()
    await r.connect()
    client = await r.get_client()
    high = await client.llen('queue:high')
    medium = await client.llen('queue:medium')
    low = await client.llen('queue:low')
    completed = await client.llen('jobs:completed')
    budget = await client.get('energy:budget:consumed')
    keys = await client.keys('energy:*')
    print(f'Queue  - HIGH: {high}, MEDIUM: {medium}, LOW: {low}')
    print(f'Completed jobs: {completed}')
    print(f'CO2 consumed: {budget}g')
    print(f'Energy keys: {keys}')
    await r.disconnect()

asyncio.run(main())
"
