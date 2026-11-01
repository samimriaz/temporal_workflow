import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path

from shopping.models import Order


class ShopRejection(Exception):
    """A terminal rejection from a simulated shopping API."""


class ShopStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with sqlite3.connect(self.path, timeout=10) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            yield db

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS inventory "
                "(sku TEXT PRIMARY KEY, available INTEGER NOT NULL CHECK(available >= 0))"
            )
            db.execute("INSERT OR IGNORE INTO inventory VALUES ('headphones', 100)")
            db.execute(
                "CREATE TABLE IF NOT EXISTS orders "
                "(id TEXT PRIMARY KEY, request TEXT NOT NULL, status TEXT NOT NULL, "
                "payment TEXT NOT NULL DEFAULT 'none', inventory TEXT NOT NULL DEFAULT 'none')"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS events "
                "(id INTEGER PRIMARY KEY, time TEXT NOT NULL DEFAULT "
                "(strftime('%Y-%m-%dT%H:%M:%fZ','now')), order_id TEXT NOT NULL, "
                "run_id TEXT NOT NULL, step TEXT NOT NULL, attempt INTEGER NOT NULL, "
                "worker TEXT NOT NULL, phase TEXT NOT NULL, detail TEXT NOT NULL)"
            )

    def record(
        self, order_id: str, run_id: str, step: str, attempt: int,
        worker: str, phase: str, detail: str = "",
    ) -> None:
        with self.transaction() as db:
            db.execute(
                "INSERT INTO events(order_id,run_id,step,attempt,worker,phase,detail) "
                "VALUES (?,?,?,?,?,?,?)",
                (order_id, run_id, step, attempt, worker, phase, detail),
            )

    def apply(self, step: str, order: Order) -> str:
        order.validate()
        request = json.dumps(asdict(order), sort_keys=True)
        with self.transaction() as db:
            row = db.execute("SELECT * FROM orders WHERE id=?", (order.order_id,)).fetchone()
            if row is not None and row["request"] != request:
                raise ShopRejection("Order ID belongs to a different request")
            if step == "receive_order":
                db.execute(
                    "INSERT OR IGNORE INTO orders(id,request,status) VALUES (?,?,'received')",
                    (order.order_id, request),
                )
                return "received" if row is None else str(row["status"])
            if row is None:
                if step == "compensate":
                    return "not-created"
                raise ShopRejection("Order has not been received")
            if step == "compensate":
                if row["status"] == "delivered":
                    return "delivered"
                if row["inventory"] == "reserved":
                    db.execute(
                        "UPDATE inventory SET available=available+? WHERE sku=?",
                        (order.quantity, order.sku),
                    )
                db.execute(
                    "UPDATE orders SET status='compensated', "
                    "payment=CASE WHEN payment='charged' THEN 'refunded' ELSE payment END, "
                    "inventory=CASE WHEN inventory='reserved' THEN 'released' ELSE inventory END "
                    "WHERE id=?", (order.order_id,),
                )
                return "compensated"
            # Settled state fences late attempts after a timeout or compensation.
            if row["status"] == "compensated":
                raise ShopRejection("Order has already been compensated")
            if row["status"] == "delivered":
                return "delivered"
            if step == "deduct_payment":
                if row["payment"] == "charged":
                    return "paid"
                if order.scenario == "invalid-payment":
                    raise ShopRejection("Invalid payment: simulated card declined")
                db.execute(
                    "UPDATE orders SET payment='charged',status='paid' WHERE id=?",
                    (order.order_id,),
                )
                return "paid"
            if step == "check_inventory":
                if row["inventory"] == "reserved":
                    return "reserved"
                if row["payment"] != "charged":
                    raise ShopRejection("Payment must be deducted before inventory reservation")
                if order.scenario == "out-of-stock":
                    raise ShopRejection("Product is out of stock")
                changed = db.execute(
                    "UPDATE inventory SET available=available-? WHERE sku=? AND available>=?",
                    (order.quantity, order.sku, order.quantity),
                ).rowcount
                if not changed:
                    raise ShopRejection("Unknown product or insufficient inventory")
                db.execute(
                    "UPDATE orders SET inventory='reserved',status='reserved' WHERE id=?",
                    (order.order_id,),
                )
                return "reserved"
            if step == "deliver_product":
                if row["payment"] != "charged" or row["inventory"] != "reserved":
                    raise ShopRejection("Payment and inventory reservation are required for delivery")
                db.execute("UPDATE orders SET status='delivered' WHERE id=?", (order.order_id,))
                return "delivered"
            raise ValueError(f"Unknown shopping operation: {step}")

    def snapshot(self, order_id: str | None = None) -> dict[str, list[dict]]:
        with self.transaction() as db:
            where = "" if order_id is None else " WHERE id=?"
            event_where = "" if order_id is None else " WHERE order_id=?"
            params = () if order_id is None else (order_id,)
            return {
                "inventory": [dict(row) for row in db.execute("SELECT * FROM inventory ORDER BY sku")],
                "orders": [
                    dict(row) for row in db.execute(
                        "SELECT id,status,payment,inventory FROM orders" + where + " ORDER BY id",
                        params,
                    )
                ],
                "events": [
                    dict(row) for row in db.execute(
                        "SELECT * FROM events" + event_where + " ORDER BY id", params,
                    )
                ],
            }
