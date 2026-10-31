import argparse
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from temporalio.client import Client
from temporalio.worker import Worker

from banking.activities import BankingActivities
from banking.ledger import Ledger
from banking.models import ACTIVITY_QUEUE, WORKFLOW_QUEUE
from banking.workflows import BankTransfer


async def run(role: str, address: str, database: Path) -> None:
    client = await Client.connect(address)
    if role == "workflow":
        logging.info("Workflow worker polling %s", WORKFLOW_QUEUE)
        await Worker(client, task_queue=WORKFLOW_QUEUE, workflows=[BankTransfer]).run()
    else:
        ledger = Ledger(database)
        ledger.initialize()
        with ThreadPoolExecutor(max_workers=4) as executor:
            activities = BankingActivities(ledger, executor)
            logging.info("Banking worker polling %s; database=%s", ACTIVITY_QUEUE, database.resolve())
            await Worker(
                client, task_queue=ACTIVITY_QUEUE,
                activities=[activities.debit, activities.credit, activities.reconcile],
            ).run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one of the bank demo workers")
    parser.add_argument("role", choices=["workflow", "banking"])
    parser.add_argument("--address", default="localhost:7233")
    parser.add_argument("--database", type=Path, default=Path("data") / "bank.db")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(args.role, args.address, args.database))


if __name__ == "__main__":
    main()
