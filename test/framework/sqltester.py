import inspect
from pathlib import Path
from typing import Type
from typing import Any
import pytest
from functools import cmp_to_key
from decimal import Decimal
import re
from conftest import Cluster, TarantoolError


def init_cluster(cluster: Cluster, instance_count: int) -> Cluster:
    cluster.deploy(instance_count=instance_count)
    for i in cluster.instances:
        i.wait_online()
    return cluster


def compare_lists(a: list, b: list):
    for x, y in zip(a, b):
        if x is None:
            return -1
        elif y is None:
            return 1
        elif x != y:
            return (x > y) - (x < y)
    return 0


def do_execsql(cluster: Cluster, query: str, expected: list):
    instance = cluster.leader()
    result = instance.sql(query)
    result_len = len(result[0]) if len(result) > 0 else 0
    new_expected = map(list, zip(*(iter(expected),) * result_len))
    assert sorted(result, key=cmp_to_key(compare_lists)) == sorted(new_expected, key=cmp_to_key(compare_lists))


def do_catchsql(cluster: Cluster, sql: str, expected: str | list):
    instance = cluster.leader()
    queries = [q.strip() for q in sql.split(";") if q.strip()]

    if isinstance(expected, str):
        expected = [expected]
    elif expected is None:
        expected = [None] * len(queries)

    assert len(queries) == len(expected), f"Mismatch: {len(queries)} SQL queries but {len(expected)} expected errors."

    for query, exp_err in zip(queries, expected):
        if exp_err and exp_err != "-":
            msg = re.escape(exp_err.strip())
            with pytest.raises(TarantoolError, match=msg):
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
        r"-- TEST: (.*?)\n"  # Test name
        r"-- SQL:\n(.*?)\n"  # SQL query
        r"(?:-- EXPECTED:\n(.*?))?"  # Expected result (optional)
        r"(?:-- ERROR:\n(.*?))?"  # Expected error (optional)
        r"(?=-- TEST:|\Z)"  # Next test or end of file
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
        explain_flag = False
        expected = None
        if query.lower().startswith("explain"):
            explain_flag = True
            expected = _parse_line(match[2], "\n", "\n") if match[2] else None
        else:
            expected = _parse_line(match[2], None, ",") if match[2] else None
        error = match[3].strip().split("\n") if match[3] else None
        assert not (expected and error), "Cannot provide both expected result and error"
        params.append(pytest.param(query, expected, explain_flag, error, id=name))
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
        metafunc.parametrize(["query", "expected", "explain_flag", "error"], metafunc.cls.params)


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

    def test_sql(self, cluster_1: Cluster, query: str, expected: list, explain_flag: bool, error: str):
        if explain_flag:
            do_explain_sql(cluster_1, query, expected)
        elif expected:
            do_execsql(cluster_1, query, expected)
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

    def test_sql(self, cluster_2: Cluster, query: str, expected: list, explain_flag: bool, error: str):
        if explain_flag:
            do_explain_sql(cluster_2, query, expected)
        elif expected:
            do_execsql(cluster_2, query, expected)
        else:
            do_catchsql(cluster_2, query, error)
