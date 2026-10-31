import sqlite3
from contextlib import contextmanager
from pathlib import Path
from collections.abc import Iterator

from banking.models import SCENARIOS, Transfer


class BankError(Exception):
    """A terminal business rejection, not a transient infrastructure error."""


class Ledger:
    def __init__(self, path: Path) -> None:
        self.path = path

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with sqlite3.connect(self.path, timeout=10) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN IMMEDIATE")
            yield connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as db:
            db.execute("CREATE TABLE IF NOT EXISTS accounts "
                       "(id TEXT PRIMARY KEY, balance_cents INTEGER NOT NULL "
                       "CHECK(balance_cents >= 0))")
            db.execute("CREATE TABLE IF NOT EXISTS transfers "
                       "(id TEXT PRIMARY KEY, source TEXT NOT NULL, destination TEXT NOT NULL, "
                       "amount_cents INTEGER NOT NULL, scenario TEXT NOT NULL, state TEXT NOT NULL)")
            db.executemany(
                "INSERT OR IGNORE INTO accounts VALUES (?, ?)",
                [("alice", 10000), ("bob", 10000)],
            )

    def _ensure(self, db: sqlite3.Connection, transfer: Transfer) -> str:
        if (
            type(transfer.amount_cents) is not int
            or transfer.amount_cents <= 0
            or transfer.amount_cents > 1_000_000_000
            or not transfer.transfer_id.strip()
            or transfer.source == transfer.destination
            or transfer.scenario not in SCENARIOS
        ):
            raise BankError("Invalid transfer request")
        for account in (transfer.source, transfer.destination):
            if not db.execute("SELECT 1 FROM accounts WHERE id = ?", (account,)).fetchone():
                raise BankError(f"Unknown account: {account}")
        values = (
            transfer.transfer_id, transfer.source, transfer.destination,
            transfer.amount_cents, transfer.scenario,
        )
        db.execute("INSERT OR IGNORE INTO transfers VALUES (?, ?, ?, ?, ?, 'pending')", values)
        row = db.execute("SELECT * FROM transfers WHERE id = ?", (transfer.transfer_id,)).fetchone()
        if tuple(row)[:5] != values:
            raise BankError("Transfer ID already belongs to a different request")
        return str(row["state"])

    def debit(self, transfer: Transfer) -> str:
        with self.transaction() as db:
            state = self._ensure(db, transfer)
            if state != "pending":
                return state
            changed = db.execute(
                "UPDATE accounts SET balance_cents = balance_cents - ? "
                "WHERE id = ? AND balance_cents >= ?",
                (transfer.amount_cents, transfer.source, transfer.amount_cents),
            ).rowcount
            if not changed:
                raise BankError("Insufficient funds")
            db.execute("UPDATE transfers SET state = 'debited' WHERE id = ?", (transfer.transfer_id,))
            return "debited"

    def credit(self, transfer: Transfer) -> str:
        with self.transaction() as db:
            state = self._ensure(db, transfer)
            if state == "completed":
                return state
            if state != "debited":
                raise BankError(f"Cannot credit transfer in state {state}")
            if transfer.scenario == "permanent":
                raise BankError("Destination bank rejected the credit")
            db.execute(
                "UPDATE accounts SET balance_cents = balance_cents + ? WHERE id = ?",
                (transfer.amount_cents, transfer.destination),
            )
            db.execute("UPDATE transfers SET state = 'completed' WHERE id = ?", (transfer.transfer_id,))
            return "completed"

    def reconcile(self, transfer: Transfer) -> str:
        with self.transaction() as db:
            state = self._ensure(db, transfer)
            if state in ("completed", "refunded", "rejected"):
                return state
            if state == "debited":
                db.execute(
                    "UPDATE accounts SET balance_cents = balance_cents + ? WHERE id = ?",
                    (transfer.amount_cents, transfer.source),
                )
                state = "refunded"
            else:
                state = "rejected"
            # A terminal state fences late debit/credit attempts after an activity timeout.
            db.execute("UPDATE transfers SET state = ? WHERE id = ?", (state, transfer.transfer_id))
            return state

    def snapshot(self) -> dict[str, list[dict]]:
        with self.transaction() as db:
            return {
                "accounts": [dict(row) for row in db.execute("SELECT * FROM accounts ORDER BY id")],
                "transfers": [dict(row) for row in db.execute("SELECT * FROM transfers ORDER BY id")],
            }
