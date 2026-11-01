import asyncio
import os
from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.runtime import Runtime, TelemetryConfig
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shopping.activities import ShoppingActivities
from shopping.models import Order
from shopping.store import ShopStore
from shopping.workflows import ShoppingOrder


@pytest_asyncio.fixture
async def environment():
    address = os.environ.get("TEMPORAL_TEST_ADDRESS")
    if address:
        yield WorkflowEnvironment.from_client(await Client.connect(address))
    else:
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=os.environ.get("TEMPORAL_CLI"),
        ) as env:
            yield env


@pytest.fixture
def store(tmp_path):
    result = ShopStore(tmp_path / "shop.db")
    result.initialize()
    return result


def worker(client, queue, store, identity):
    activities = ShoppingActivities(store, identity)
    return Worker(
        client, task_queue=queue, identity=identity,
        workflows=[ShoppingOrder], activities=activities.registered(),
        max_concurrent_activities=1, graceful_shutdown_timeout=timedelta(seconds=0),
    )


async def wait_for_event(store, order_id, step, phase):
    async def poll():
        while True:
            events = await asyncio.to_thread(store.snapshot, order_id)
            matches = [
                event for event in events["events"]
                if event["step"] == step and event["phase"] == phase
            ]
            if matches:
                return matches
            await asyncio.sleep(0.1)
    return await asyncio.wait_for(poll(), timeout=30)


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["success", "transient"])
async def test_success_and_retry(environment, store, scenario):
    queue = f"test-{uuid4()}"
    order = Order(str(uuid4()), scenario=scenario, step_seconds=0)
    async with worker(environment.client, queue, store, "test-worker"):
        handle = await environment.client.start_workflow(
            ShoppingOrder.run, order, id=f"order-{order.order_id}", task_queue=queue,
        )
        result = await asyncio.wait_for(handle.result(), timeout=30)
        assert result.status == "delivered"
        history = await handle.fetch_history()
    scheduled = [
        event.activity_task_scheduled_event_attributes for event in history.events
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
    ]
    assert len(scheduled) == 4
    assert all(event.schedule_to_close_timeout.seconds == 1800 for event in scheduled)
    assert all(event.retry_policy.maximum_attempts == 0 for event in scheduled)
    if scenario == "transient":
        events = store.snapshot()["events"]
        attempts = [
            event["attempt"] for event in events
            if event["step"] == "check_inventory" and event["phase"] == "started"
        ]
        assert attempts == [1, 2, 3]
    assert store.snapshot()["inventory"][0]["available"] == 99


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario,fail_step", [
    ("invalid-payment", "deduct_payment"),
    ("out-of-stock", "check_inventory"),
    ("api-down", "check_inventory"),
    ("api-down", "deliver_product"),
])
async def test_terminal_failures_and_retry_budget(environment, store, scenario, fail_step):
    queue = f"test-{uuid4()}"
    order = Order(
        str(uuid4()), scenario=scenario, fail_step=fail_step, step_seconds=0, retry_seconds=8,
    )
    async with worker(environment.client, queue, store, "failure-worker"):
        with pytest.raises(WorkflowFailureError) as failure:
            await asyncio.wait_for(environment.client.execute_workflow(
                ShoppingOrder.run, order, id=f"order-{order.order_id}", task_queue=queue,
            ), timeout=30)
    assert isinstance(failure.value.cause, ApplicationError)
    assert failure.value.cause.type == "OrderFailed"
    state = store.snapshot()["orders"][0]
    assert state["status"] == "compensated"
    assert state["payment"] == ("none" if scenario == "invalid-payment" else "refunded")
    assert store.snapshot()["inventory"][0]["available"] == 100
    attempts = [
        event for event in store.snapshot()["events"]
        if event["step"] == fail_step and event["phase"] == "started"
    ]
    assert len(attempts) == (3 if scenario == "api-down" else 1)


@pytest.mark.asyncio
async def test_restart_and_takeover_after_unacknowledged_commit(environment, store):
    queue = f"test-{uuid4()}"
    order = Order(str(uuid4()), step_seconds=0, acknowledgement_seconds=6)
    first = worker(environment.client, queue, store, "worker-A")
    async with first:
        handle = await environment.client.start_workflow(
            ShoppingOrder.run, order, id=f"order-{order.order_id}", task_queue=queue,
        )
        await wait_for_event(store, order.order_id, "deduct_payment", "committed")
    before = store.snapshot(order.order_id)
    assert before["orders"][0]["payment"] == "charged"
    async with worker(environment.client, queue, store, "worker-B"):
        assert (await asyncio.wait_for(handle.result(), timeout=60)).status == "delivered"
    events = store.snapshot(order.order_id)["events"]
    payment_attempts = [
        event for event in events
        if event["step"] == "deduct_payment" and event["phase"] == "started"
    ]
    assert payment_attempts[0]["worker"] == "worker-A"
    assert payment_attempts[-1]["worker"] == "worker-B"
    assert payment_attempts[-1]["attempt"] >= 2
    receive_attempts = [
        event for event in events
        if event["step"] == "receive_order" and event["phase"] == "started"
    ]
    assert len(receive_attempts) == 1
    assert store.snapshot()["inventory"][0]["available"] == 99


@pytest.mark.asyncio
async def test_two_workers_process_two_orders_concurrently(environment, store):
    queue = f"test-{uuid4()}"
    second_client = await Client.connect(
        environment.client.service_client.config.target_host,
        runtime=Runtime(telemetry=TelemetryConfig()),
    )
    async with (
        worker(environment.client, queue, store, "parallel-A"),
        worker(second_client, queue, store, "parallel-B"),
    ):
        handles = [
            await environment.client.start_workflow(
                ShoppingOrder.run, Order(str(uuid4()), step_seconds=1),
                id=f"parallel-{uuid4()}", task_queue=queue,
            )
            for _ in range(2)
        ]
        results = await asyncio.wait_for(asyncio.gather(
            *(handle.result() for handle in handles),
        ), timeout=30)
    assert all(result.status == "delivered" for result in results)
    events = store.snapshot()["events"]
    assert {event["worker"] for event in events} == {"parallel-A", "parallel-B"}
    started = [event for event in events if event["phase"] == "started"]
    completed = [event for event in events if event["phase"] == "completed"]
    assert started[1]["time"] < completed[0]["time"]
    assert started[0]["order_id"] != started[1]["order_id"]
    assert store.snapshot()["inventory"][0]["available"] == 98
