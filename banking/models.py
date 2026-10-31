from dataclasses import dataclass
from typing import Literal

Scenario = Literal["success", "transient", "permanent", "crash-credit"]
SCENARIOS: tuple[Scenario, ...] = ("success", "transient", "permanent", "crash-credit")
WORKFLOW_QUEUE = "banking-workflows"
ACTIVITY_QUEUE = "banking-activities"


@dataclass
class Transfer:
    transfer_id: str
    source: str = "alice"
    destination: str = "bob"
    amount_cents: int = 1000
    scenario: Scenario = "success"


@dataclass
class TransferResult:
    transfer_id: str
    status: str
    detail: str
