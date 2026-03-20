import asyncio
import logging

from broker.queue_manager import QueueManager
from scheduler.scheduler import Scheduler

logger = logging.getLogger(__name__)

FLUSHER_INTERVAL_SECONDS = 60


async def _flusher_loop(scheduler: Scheduler, queue_manager: QueueManager) -> None:
    """Periodically evaluate whether deferred jobs should be flushed."""
    # queue_manager is intentionally part of the flusher interface so this
    # loop can evolve to perform actual queue flush operations.
    _ = queue_manager
    while True:
        should_flush = await scheduler.should_flush_deferred()
        if should_flush:
            logger.info("Grid clean — flushing deferred jobs")
        else:
            logger.info("Grid still dirty — keeping jobs deferred")
        await asyncio.sleep(FLUSHER_INTERVAL_SECONDS)


async def start_flusher(
    scheduler: Scheduler, queue_manager: QueueManager
) -> asyncio.Task[None]:
    """Start the queue flusher as a background task."""
    return asyncio.create_task(
        _flusher_loop(scheduler, queue_manager),
        name="queue-flusher",
    )
