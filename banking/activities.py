import asyncio
from concurrent.futures import ThreadPoolExecutor

from temporalio import activity
from temporalio.exceptions import ApplicationError

from banking.ledger import BankError, Ledger
from banking.models import Transfer


class BankingActivities:
    def __init__(self, ledger: Ledger, executor: ThreadPoolExecutor) -> None:
        self.ledger = ledger
        self.executor = executor

    async def _run(self, operation: str, transfer: Transfer) -> str:
        try:
            function = {
                "debit": self.ledger.debit,
                "credit": self.ledger.credit,
                "reconcile": self.ledger.reconcile,
            }[operation]
            return await asyncio.get_running_loop().run_in_executor(
                self.executor, function, transfer
            )
        except BankError as error:
            raise ApplicationError(str(error), type="BankRejection", non_retryable=True) from error

    @activity.defn
    async def debit(self, transfer: Transfer) -> str:
        activity.logger.info("Debiting %s; attempt %s", transfer.transfer_id, activity.info().attempt)
        return await self._run("debit", transfer)

    @activity.defn
    async def credit(self, transfer: Transfer) -> str:
        attempt = activity.info().attempt
        activity.logger.info("Crediting %s; attempt %s", transfer.transfer_id, attempt)
        if transfer.scenario == "transient" and attempt <= 2:
            raise ApplicationError("Simulated bank outage", type="BankUnavailable")
        state = await self._run("credit", transfer)
        if transfer.scenario == "crash-credit" and attempt == 1:
            activity.logger.warning(
                "CREDIT COMMITTED for %s. Stop this banking worker now; "
                "delaying acknowledgement for 20 seconds.", transfer.transfer_id
            )
            await asyncio.sleep(20)
        return state

    @activity.defn
    async def reconcile(self, transfer: Transfer) -> str:
        activity.logger.warning("Reconciling/refunding %s", transfer.transfer_id)
        return await self._run("reconcile", transfer)
