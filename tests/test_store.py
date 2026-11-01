from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from shopping.models import STEPS, Order
from shopping.store import ShopRejection, ShopStore


@pytest.fixture
def store(tmp_path):
    result = ShopStore(tmp_path / "shop.db")
    result.initialize()
    return result


def test_success_and_duplicate_operations(store):
    order = Order("success")
    for step in STEPS:
        store.apply(step, order)
        store.apply(step, order)
    assert store.snapshot()["orders"] == [
        {"id": "success", "status": "delivered", "payment": "charged", "inventory": "reserved"},
    ]
    assert store.snapshot()["inventory"][0]["available"] == 99
    assert store.apply("compensate", order) == "delivered"
    assert store.snapshot()["inventory"][0]["available"] == 99


@pytest.mark.parametrize("last_step", ["receive_order", "deduct_payment", "check_inventory"])
def test_compensation_is_idempotent_and_fences_late_attempts(store, last_step):
    order = Order("refund")
    for step in STEPS[:STEPS.index(last_step) + 1]:
        store.apply(step, order)
    assert store.apply("compensate", order) == "compensated"
    assert store.apply("compensate", order) == "compensated"
    state = store.snapshot()["orders"][0]
    assert state["status"] == "compensated"
    assert state["payment"] == ("none" if last_step == "receive_order" else "refunded")
    assert store.snapshot()["inventory"][0]["available"] == 100
    with pytest.raises(ShopRejection, match="compensated"):
        store.apply("deliver_product", order)


def test_invalid_payment_never_charges(store):
    order = Order("declined", scenario="invalid-payment")
    store.apply("receive_order", order)
    with pytest.raises(ShopRejection, match="Invalid payment"):
        store.apply("deduct_payment", order)
    assert store.snapshot()["orders"][0]["payment"] == "none"


def test_changed_request_rejected(store):
    order = Order("duplicate")
    store.apply("receive_order", order)
    with pytest.raises(ShopRejection, match="different request"):
        store.apply("receive_order", replace(order, amount_cents=999))


def test_concurrent_reservations_do_not_oversell(store):
    orders = [Order(f"order-{i}", quantity=60) for i in range(2)]
    for order in orders:
        store.apply("receive_order", order)
        store.apply("deduct_payment", order)

    def reserve(order):
        try:
            return store.apply("check_inventory", order)
        except ShopRejection as error:
            return str(error)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, orders))
    assert results.count("reserved") == 1
    assert store.snapshot()["inventory"][0]["available"] == 40


@pytest.mark.parametrize("changes", [
    {"quantity": 0}, {"amount_cents": -1}, {"order_id": " "},
    {"retry_seconds": 1801}, {"step_seconds": float("nan")},
    {"acknowledgement_seconds": float("inf")}, {"retry_seconds": 2},
])
def test_invalid_request_rejected(changes):
    with pytest.raises(ValueError):
        replace(Order("input"), **changes).validate()
