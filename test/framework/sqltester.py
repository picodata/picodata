import inspect
from collections import Counter
from pathlib import Path
from typing import Type
from typing import Any
import pytest
from decimal import Decimal
import re
from conftest import Cluster, TarantoolError


NOT_AN_ERROR = "-"


def init_cluster(cluster: Cluster, instance_count: int) -> Cluster:
    cluster.deploy(instance_count=instance_count)
    for i in cluster.instances:
        i.wait_online()
    return cluster


def compare_results(expected, actual):
    expected_counter = Counter(map(tuple, expected))
    actual_counter = Counter(map(tuple, actual))

    missing = expected_counter - actual_counter
    unexpected = actual_counter - expected_counter

    if missing or unexpected:
        message = ["Mismatch in SQL test results:"]
        if missing:
            message.append("\nMissing tuples:")
            for tup, count in missing.items():
                message.append(f"  {tup} (x{count})")
        if unexpected:
            message.append("\nUnexpected tuples:")
            for tup, count in unexpected.items():
                message.append(f"  {tup} (x{count})")
        raise AssertionError("\n".join(message))


def do_execsql(cluster: Cluster, query: str, expected: list):
    instance = cluster.leader()
    result = instance.sql(query)
    result_len = len(result[0]) if len(result) > 0 else 0
    new_expected = list(map(list, zip(*(iter(expected),) * result_len)))
    compare_results(new_expected, result)


def do_execsql_exact(cluster: Cluster, query: str, expected: list):
    instance = cluster.leader()
    result = instance.sql(query)
    data = [col for row in result for col in row]
    assert data == expected


def do_catchsql(cluster: Cluster, sql: str, expected: str | list):
    instance = cluster.leader()
    queries = [q.strip() for q in sql.split(";") if q.strip()]

    if isinstance(expected, str):
        expected = [expected]
    elif expected is None:
        expected = [None] * len(queries)

    assert len(queries) == len(expected), f"Mismatch: {len(queries)} SQL queries but {len(expected)} expected errors."

    for query, exp_err in zip(queries, expected):
        if exp_err and exp_err != NOT_AN_ERROR:
            with pytest.raises(TarantoolError, match=exp_err):
                instance.sql(query)
        else:
            instance.sql(query)


def do_explain_sql(cluster: Cluster, query: str, expected: list):
    instance = cluster.leader()
    result = instance.sql(query)
    assert result == expected


def _parse_line(input_string, lead_sym: Any, split_by: str):
    result: list[Any] = []
    elements = input_string.split(split_by)

    for element in elements:
        if (
            element.lower() != ""
            and element.lower()[0] == " "
            and element.lower() == len(element.lower()) * element.lower()[0]
        ):
            result.append(None)
            continue

        element = element.strip(lead_sym)  # Remove leading and trailing whitespaces

        if element.lower() == "":
            continue

        elif element.startswith("'") and element.endswith("'"):
            # If element in single quotes, remove them and add as string
            result.append(element[1:-1])
        elif element.lower() in {"null", "none", "nil"}:
            # If element is a null, add as None
            result.append(None)
        elif element.isdigit() or (element[0] == "-" and element[1:].isdigit()):
            # If element is a number, add as int
            result.append(int(element))
        elif element.lower() == "true" or element.lower() == "false":
            result.append(bool(element.lower() == "true"))
        elif element.find("Decimal") != -1:
            result.append(Decimal(element[9:-2]))
        else:
            try:
                # Try to convert element to float
                result.append(float(element))
            except ValueError:
                # If not possible, add as string
                result.append(element)

    return result


def parse_file(cls: Type, file_name: str) -> list:
    test_file = Path(inspect.getfile(cls)).parent / file_name
    content = test_file.read_text()
    test_pattern = (
        # Test name
        r"-- TEST: ([^\n]*)\n"
        # SQL query
        r"-- SQL:\n(.*?)\n"
        # EXPECTED (optional): tuples returned by the picodata must
        # appear in exactly the same order as specified in the test
        # result.
        # UNORDERED (optional): tuples returned by the picodata have
        # no ordering guarantees. Expected and actual values will be
        # sorted before comparison.
        # ERROR (optional): error message that must be raised by the
        # picodata.
        r"(?:-- (EXPECTED|UNORDERED|ERROR):\n(.*?))?"
        # Next test or end of file
        r"(?=-- TEST:|\Z)"
    )
    matches = re.findall(test_pattern, content, re.DOTALL)
    params = []
    for match in matches:
        name = match[0].strip()
        assert name, "Test name must be provided"
        query = match[1]
        if query.startswith("--"):
            continue
        query = query.strip()
        assert query, "SQL query must be provided"
        error = None
        expected = None
        is_exact: bool = False
        if match[2] == "ERROR":
            error = match[3].strip().split("\n")
        elif match[2] == "EXPECTED" or match[2] == "UNORDERED":
            is_exact = True if match[2] == "EXPECTED" else False
            if query.lower().startswith("explain"):
                expected = _parse_line(match[3], "\n", "\n")
            else:
                expected = _parse_line(match[3], None, ",")
        params.append(pytest.param(query, expected, is_exact, error, id=name))
    return params


def sql_test_file(file_name: str):
    def inner(cls):
        cls = pytest.mark.xdist_group(name=f"{file_name}#{cls.__name__}")(cls)
        cls.params = parse_file(cls, file_name)
        return cls

    return inner


# Pytest hook to generate tests from parameters. For details see test/framework/sqltester.py
def pytest_generate_tests(metafunc):
    if metafunc.function.__name__ == "test_sql":
        assert "query" in metafunc.fixturenames
        assert "expected" in metafunc.fixturenames
        assert "error" in metafunc.fixturenames
        metafunc.parametrize(["query", "expected", "is_exact", "error"], metafunc.cls.params)


@pytest.fixture(scope="class")
def cluster_1(cluster: Cluster):
    init_cluster(cluster, 1)
    assert len(cluster.instances) == 1
    for i in cluster.instances:
        cluster.wait_until_instance_has_this_many_active_buckets(i, 3000)
    yield cluster
    cluster.kill()


class ClusterSingleInstance:
    params: list = []

    def test_sql(self, cluster_1: Cluster, query: str, is_exact: bool, expected: list, error: str):
        if query.lower().startswith("explain") and not error:
            do_explain_sql(cluster_1, query, expected)
        elif expected and not is_exact:
            do_execsql(cluster_1, query, expected)
        elif expected and is_exact:
            do_execsql_exact(cluster_1, query, expected)
        else:
            do_catchsql(cluster_1, query, error)


@pytest.fixture(scope="class")
def cluster_2(cluster: Cluster):
    init_cluster(cluster, 2)
    assert len(cluster.instances) == 2
    for i in cluster.instances:
        cluster.wait_until_instance_has_this_many_active_buckets(i, 1500)
    yield cluster
    cluster.kill()


class ClusterTwoInstances:
    params: list = []

    def test_sql(self, cluster_2: Cluster, query: str, expected: list, is_exact: bool, error: str):
        if query.lower().startswith("explain") and not error:
            do_explain_sql(cluster_2, query, expected)
        elif expected and not is_exact:
            do_execsql(cluster_2, query, expected)
        elif expected and is_exact:
            do_execsql_exact(cluster_2, query, expected)
        else:
            do_catchsql(cluster_2, query, error)
