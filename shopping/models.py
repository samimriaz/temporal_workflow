from dataclasses import dataclass
from typing import Literal

TASK_QUEUE = "shopping-orders"
STEPS = ("receive_order", "deduct_payment", "check_inventory", "deliver_product")
Scenario = Literal["success", "transient", "api-down", "invalid-payment", "out-of-stock"]
SCENARIOS: tuple[Scenario, ...] = (
    "success", "transient", "api-down", "invalid-payment", "out-of-stock",
)


@dataclass
class Order:
    order_id: str
    sku: str = "headphones"
    quantity: int = 1
    amount_cents: int = 2500
    scenario: Scenario = "success"
    fail_step: str = "check_inventory"
    step_seconds: float = 4
    retry_seconds: float = 1800
    acknowledgement_seconds: float = 0

    def validate(self) -> None:
        import math

        if not self.order_id.strip() or not self.sku.strip():
            raise ValueError("Order ID and SKU must not be blank")
        if type(self.quantity) is not int or not 1 <= self.quantity <= 100:
            raise ValueError("Quantity must be an integer between 1 and 100")
        if type(self.amount_cents) is not int or not 1 <= self.amount_cents <= 1_000_000_000:
            raise ValueError("Amount must be integer cents between 1 and 1000000000")
        if self.scenario not in SCENARIOS or self.fail_step not in STEPS[1:]:
            raise ValueError("Unknown scenario or failing API step")
        times = (self.step_seconds, self.retry_seconds, self.acknowledgement_seconds)
        if any(not math.isfinite(value) for value in times):
            raise ValueError("Timing values must be finite")
        if not 0 <= self.step_seconds <= 60 or not 0 <= self.acknowledgement_seconds <= 60:
            raise ValueError("Step and acknowledgement delays must be between 0 and 60 seconds")
        if not self.step_seconds + self.acknowledgement_seconds + 2 <= self.retry_seconds <= 1800:
            raise ValueError("Retry budget must cover delays plus 2 seconds, and be at most 1800")


@dataclass
class OrderResult:
    order_id: str
    status: str
