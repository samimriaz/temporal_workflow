from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from banking.models import ACTIVITY_QUEUE, Transfer, TransferResult


@workflow.defn
class BankTransfer:
    @workflow.run
    async def run(self, transfer: Transfer) -> TransferResult:
        retry = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2,
            maximum_interval=timedelta(seconds=4),
            maximum_attempts=4,
        )
        try:
            state = await workflow.execute_activity(
                "debit", transfer, result_type=str, task_queue=ACTIVITY_QUEUE,
                start_to_close_timeout=timedelta(seconds=30),
                schedule_to_close_timeout=timedelta(minutes=5), retry_policy=retry,
            )
            if state in ("refunded", "rejected"):
                return TransferResult(transfer.transfer_id, state, "Already settled")
            await workflow.execute_activity(
                "credit", transfer, result_type=str, task_queue=ACTIVITY_QUEUE,
                start_to_close_timeout=timedelta(seconds=30),
                schedule_to_close_timeout=timedelta(minutes=5), retry_policy=retry,
            )
            return TransferResult(transfer.transfer_id, "completed", "Destination credited")
        except ActivityError as error:
            workflow.logger.warning("Bank operation failed; reconciling: %s", error)
            # Do not assume a timeout means the bank did not commit.
            state = await workflow.execute_activity(
                "reconcile", transfer, result_type=str, task_queue=ACTIVITY_QUEUE,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_interval=timedelta(seconds=10)),
            )
            return TransferResult(transfer.transfer_id, state, f"Reconciled after: {error.cause}")
