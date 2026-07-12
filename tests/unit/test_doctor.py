from types import SimpleNamespace

from codegraph.config.models import ScipConfig
from codegraph.doctor import CheckResult, run_env_checks, run_store_probes


class FakeGraph:
    def __init__(self, fail_on: set[str]):
        self.fail_on = fail_on
        self.queries: list[str] = []

    def query(self, q: str, params=None):
        self.queries.append(q)
        for marker in self.fail_on:
            if marker in q:
                raise RuntimeError(f"unsupported: {marker}")
        return SimpleNamespace(result_set=[[1]])

    def delete(self):
        pass


class FakeDB:
    def __init__(self, fail_on: set[str] = frozenset(), constraint_ok: bool = True):
        self._g = FakeGraph(set(fail_on))
        self.constraint_ok = constraint_ok
        self.connection = self

    def select_graph(self, name: str):
        return self._g

    def ping(self):
        return True

    def execute_command(self, *args):
        if not self.constraint_ok:
            raise RuntimeError("constraints unsupported")
        return "OK"


def test_env_checks_include_python_and_node():
    results = run_env_checks(ScipConfig(), probe_scip=False)
    names = [r.name for r in results]
    assert "python" in names and "node" in names and "npx" in names
    py = next(r for r in results if r.name == "python")
    assert py.ok  # мы на 3.12+


def test_store_probes_all_ok_with_fake():
    results = run_store_probes(lambda: FakeDB())
    assert all(r.ok for r in results), [(r.name, r.detail) for r in results]
    names = {r.name for r in results}
    assert {"ping", "multi_label", "set_plus_eq", "unique_constraint",
            "vector_index_cosine", "fulltext"} <= names


def test_store_probe_failure_is_isolated():
    results = run_store_probes(lambda: FakeDB(fail_on={"VECTOR INDEX"}))
    by_name = {r.name: r for r in results}
    assert not by_name["vector_index_cosine"].ok
    assert by_name["fulltext"].ok  # остальные probes не пострадали


def test_constraint_failure_reported():
    results = run_store_probes(lambda: FakeDB(constraint_ok=False))
    by_name = {r.name: r for r in results}
    assert not by_name["unique_constraint"].ok
    assert isinstance(by_name["unique_constraint"], CheckResult)
