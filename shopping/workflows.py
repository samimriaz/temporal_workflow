from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from shopping.models import STEPS, Order, OrderResult


@workflow.defn
class ShoppingOrder:
    @workflow.run
    async def run(self, order: Order) -> OrderResult:
        try:
            order.validate()
        except ValueError as error:
            raise ApplicationError(str(error), type="InvalidOrder", non_retryable=True) from error
        retry = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=0,
        )
        try:
            for step in STEPS:
                await workflow.execute_activity(
                    step, order, result_type=str,
                    start_to_close_timeout=timedelta(
                        seconds=min(
                            order.retry_seconds,
                            max(20, order.step_seconds + order.acknowledgement_seconds + 10),
                        )
                    ),
                    schedule_to_close_timeout=timedelta(seconds=order.retry_seconds),
                    heartbeat_timeout=timedelta(seconds=5),
                    retry_policy=retry,
                )
            return OrderResult(order.order_id, "delivered")
        except ActivityError as error:
            workflow.logger.error("Order %s failed; compensating: %s", order.order_id, error)
            state = await workflow.execute_activity(
                "compensate", order, result_type=str,
                start_to_close_timeout=timedelta(seconds=20),
                heartbeat_timeout=timedelta(seconds=5),
                retry_policy=retry,
            )
            if state == "delivered":
                return OrderResult(order.order_id, "delivered")
            raise ApplicationError(
                f"Order {order.order_id} failed; settlement={state}; cause={error.cause}",
                type="OrderFailed", non_retryable=True,
            ) from error
