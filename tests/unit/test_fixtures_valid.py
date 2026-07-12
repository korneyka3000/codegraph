import ast
from pathlib import Path

FIXTURES = Path(__file__).parents[2] / "fixtures" / "services"

EXPECTED_SERVICES = {"orders_api", "kyc_worker", "document_management"}


def test_fixture_services_exist():
    assert {p.name for p in FIXTURES.iterdir() if p.is_dir()} == EXPECTED_SERVICES


def test_all_fixture_files_parse():
    py_files = list(FIXTURES.rglob("*.py"))
    assert len(py_files) >= 20
    for f in py_files:
        ast.parse(f.read_text(), filename=str(f))


def test_key_symbols_present():
    orders = (FIXTURES / "orders_api/app/services/order.py").read_text()
    assert "add_event" in orders and "OrderCreated" in orders
    worker = (FIXTURES / "kyc_worker/app/consumers/orders.py").read_text()
    assert "register_handlers" in worker
    client = (
        FIXTURES / "kyc_worker/app/clients/document_management_client.py"
    ).read_text()
    assert "class DocumentManagementClient" in client
