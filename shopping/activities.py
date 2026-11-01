import asyncio
from collections.abc import Awaitable, Callable

from temporalio import activity
from temporalio.exceptions import ApplicationError

from shopping.models import Order
from shopping.store import ShopRejection, ShopStore


class ShoppingActivities:
    def __init__(self, store: ShopStore, identity: str) -> None:
        self.store = store
        self.identity = identity

    async def _delay(self, seconds: float) -> None:
        remaining = seconds
        while remaining > 0:
            activity.heartbeat(self.identity)
            interval = min(remaining, 1)
            await asyncio.sleep(interval)
            remaining -= interval

    async def _call(self, step: str, order: Order) -> str:
        info = activity.info()

        async def record(phase: str, detail: str = "") -> None:
            await asyncio.to_thread(
                self.store.record, order.order_id, info.workflow_run_id, step,
                info.attempt, self.identity, phase, detail,
            )

        await record("started")
        activity.logger.info(
            "worker=%s order=%s step=%s attempt=%s",
            self.identity, order.order_id, step, info.attempt,
        )
        try:
            activity.heartbeat(self.identity)
            if step != "compensate":
                if step == "deduct_payment" and order.scenario == "invalid-payment":
                    raise ShopRejection("Invalid payment: simulated card declined")
                await self._delay(order.step_seconds)
                if step == order.fail_step and (
                    order.scenario == "api-down"
                    or (order.scenario == "transient" and info.attempt <= 2)
                ):
                    raise ApplicationError(
                        f"Simulated {step} API unavailable", type="ApiUnavailable",
                    )
            state = await asyncio.to_thread(self.store.apply, step, order)
            await record("committed", state)
            activity.logger.info(
                "COMMITTED worker=%s order=%s step=%s state=%s acknowledgement_delay=%ss",
                self.identity, order.order_id, step, state, order.acknowledgement_seconds,
            )
            if step != "compensate":
                await self._delay(order.acknowledgement_seconds)
            await record("completed", state)
            return state
        except ShopRejection as error:
            await record("rejected", str(error))
            raise ApplicationError(
                str(error), type="ShopRejection", non_retryable=True,
            ) from error
        except ApplicationError as error:
            await record("failed", str(error))
            raise

    @activity.defn
    async def receive_order(self, order: Order) -> str:
        return await self._call("receive_order", order)

    @activity.defn
    async def deduct_payment(self, order: Order) -> str:
        return await self._call("deduct_payment", order)

    @activity.defn
    async def check_inventory(self, order: Order) -> str:
        return await self._call("check_inventory", order)

    @activity.defn
    async def deliver_product(self, order: Order) -> str:
        return await self._call("deliver_product", order)

    @activity.defn
    async def compensate(self, order: Order) -> str:
        return await self._call("compensate", order)

    def registered(self) -> list[Callable[[Order], Awaitable[str]]]:
        return [
            self.receive_order, self.deduct_payment, self.check_inventory,
            self.deliver_product, self.compensate,
        ]
