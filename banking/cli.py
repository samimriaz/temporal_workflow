import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from temporalio.client import Client, WorkflowIDReusePolicy

from banking.ledger import Ledger
from banking.models import SCENARIOS, WORKFLOW_QUEUE, Transfer
from banking.workflows import BankTransfer


async def start(args: argparse.Namespace) -> None:
    client = await Client.connect(args.address)
    transfer = Transfer(
        transfer_id=args.id or str(uuid4()), source=args.source,
        destination=args.destination, amount_cents=args.amount_cents, scenario=args.scenario,
    )
    handle = await client.start_workflow(
        BankTransfer.run, transfer, id=f"transfer-{transfer.transfer_id}",
        task_queue=WORKFLOW_QUEUE, id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
    )
    print(f"Started {handle.id}", flush=True)
    print(json.dumps(asdict(await handle.result()), indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulated bank transfer demo")
    subparsers = parser.add_subparsers(dest="command", required=True)
    send = subparsers.add_parser("transfer")
    send.add_argument("--scenario", choices=SCENARIOS, default="success")
    send.add_argument("--id", help="Unique transfer ID; defaults to a UUID")
    send.add_argument("--source", default="alice")
    send.add_argument("--destination", default="bob")
    send.add_argument("--amount-cents", type=int, default=1000)
    send.add_argument("--address", default="localhost:7233")
    for command in ("init", "balances"):
        subparsers.add_parser(command).add_argument(
            "--database", type=Path, default=Path("data") / "bank.db"
        )
    args = parser.parse_args()
    if args.command == "transfer":
        if args.amount_cents <= 0 or args.amount_cents > 1_000_000_000:
            parser.error("--amount-cents must be between 1 and 1000000000")
        if args.source == args.destination:
            parser.error("source and destination must differ")
        asyncio.run(start(args))
    else:
        ledger = Ledger(args.database)
        if args.command == "init":
            ledger.initialize()
        print(json.dumps(ledger.snapshot(), indent=2))


if __name__ == "__main__":
    main()
