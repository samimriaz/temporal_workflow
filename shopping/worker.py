import argparse
import asyncio
import logging
import os
import socket
from datetime import timedelta
from pathlib import Path

from temporalio.client import Client
from temporalio.worker import Worker

from shopping.activities import ShoppingActivities
from shopping.models import TASK_QUEUE
from shopping.store import ShopStore
from shopping.workflows import ShoppingOrder

logger = logging.getLogger(__name__)


async def run(identity: str, address: str, database: Path, task_queue: str) -> None:
    store = ShopStore(database)
    await asyncio.to_thread(store.initialize)
    client = await Client.connect(address, identity=identity)
    activities = ShoppingActivities(store, identity)
    async with Worker(
        client, task_queue=task_queue, identity=identity,
        workflows=[ShoppingOrder], activities=activities.registered(),
        max_concurrent_activities=1,
        graceful_shutdown_timeout=timedelta(seconds=0),
    ):
        logger.info(
            "READY worker=%s pid=%s queue=%s database=%s",
            identity, os.getpid(), task_queue, database.resolve(),
        )
        await asyncio.Event().wait()


def main() -> None:
    parser = argparse.ArgumentParser(description="Combined shopping workflow and activity worker")
    parser.add_argument("--identity", default=f"shop-{socket.gethostname()}-{os.getpid()}")
    parser.add_argument("--address", default="localhost:7233")
    parser.add_argument("--database", type=Path, default=Path("data") / "shop.db")
    parser.add_argument("--task-queue", default=TASK_QUEUE)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(args.identity, args.address, args.database, args.task_queue))


if __name__ == "__main__":
    main()
