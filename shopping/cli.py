import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from temporalio.client import Client, WorkflowFailureError
from temporalio.common import WorkflowIDReusePolicy

from shopping.models import SCENARIOS, STEPS, TASK_QUEUE, Order
from shopping.store import ShopStore
from shopping.workflows import ShoppingOrder


async def submit(args: argparse.Namespace, order: Order) -> None:
    client = await Client.connect(args.address)
    handle = await client.start_workflow(
        ShoppingOrder.run, order, id=f"order-{order.order_id}", task_queue=args.task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
    )
    print(
        json.dumps({"workflow_id": handle.id, "run_id": handle.result_run_id}), flush=True,
    )
    if args.wait:
        try:
            print(json.dumps(asdict(await handle.result()), indent=2))
        except WorkflowFailureError as error:
            print(f"Order failed: {error.cause}", file=sys.stderr)
            raise SystemExit(1) from error


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulated shopping order demo")
    subparsers = parser.add_subparsers(dest="command", required=True)
    send = subparsers.add_parser("order")
    send.add_argument("--id", default=None)
    send.add_argument("--sku", default="headphones")
    send.add_argument("--quantity", type=int, default=1)
    send.add_argument("--amount-cents", type=int, default=2500)
    send.add_argument("--scenario", choices=SCENARIOS, default="success")
    send.add_argument("--fail-step", choices=STEPS[1:], default="check_inventory")
    send.add_argument("--step-seconds", type=float, default=4)
    send.add_argument("--retry-seconds", type=float, default=1800)
    send.add_argument("--acknowledgement-seconds", type=float, default=0)
    send.add_argument("--address", default="localhost:7233")
    send.add_argument("--task-queue", default=TASK_QUEUE)
    send.add_argument("--wait", action="store_true", help="Wait for result; exit 1 on workflow failure")
    for command in ("init", "inspect"):
        sub = subparsers.add_parser(command)
        sub.add_argument("--database", type=Path, default=Path("data") / "shop.db")
        if command == "inspect":
            sub.add_argument("--id")
    args = parser.parse_args()
    if args.command == "order":
        order = Order(
            order_id=args.id or str(uuid4()), sku=args.sku, quantity=args.quantity,
            amount_cents=args.amount_cents, scenario=args.scenario, fail_step=args.fail_step,
            step_seconds=args.step_seconds, retry_seconds=args.retry_seconds,
            acknowledgement_seconds=args.acknowledgement_seconds,
        )
        try:
            order.validate()
        except ValueError as error:
            parser.error(str(error))
        asyncio.run(submit(args, order))
    else:
        store = ShopStore(args.database)
        if args.command == "init":
            store.initialize()
        print(json.dumps(store.snapshot(getattr(args, "id", None)), indent=2))


if __name__ == "__main__":
    main()
