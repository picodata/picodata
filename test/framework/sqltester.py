import inspect
from collections import Counter
from pathlib import Path
from typing import Type
from typing import Any
import pytest
from decimal import Decimal
import re
from conftest import Cluster, TarantoolError
import psycopg


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


class IprotoRunner:
    def __init__(self, cluster: Cluster):
        self.cluster = cluster

    def do_execsql(self, query: str, expected: list):
        result = self.cluster.leader().sql(query)
        result_len = len(result[0]) if len(result) > 0 else 0
        new_expected = list(map(list, zip(*(iter(expected),) * result_len)))
        compare_results(new_expected, result)

    def do_execsql_exact(self, query: str, expected: list):
        result = self.cluster.leader().sql(query)
        data = [col for row in result for col in row]
        assert data == expected

    def do_explain_sql(self, query: str, expected: list):
        instance = self.cluster.leader()
        result = instance.sql(query)
        assert result == expected

    def do_catchsql(self, sql: str, expected: str | list):
        cluster = self.cluster
        instance = cluster.leader()
        queries = [q.strip() for q in sql.split(";") if q.strip()]

        if isinstance(expected, str):
            expected = [expected]
        elif expected is None:
            expected = [None] * len(queries)

        assert len(queries) == len(expected), (
            f"Mismatch: {len(queries)} SQL queries but {len(expected)} expected errors."
        )

        for query, exp_err in zip(queries, expected):
            if exp_err and exp_err != NOT_AN_ERROR:
                with pytest.raises(TarantoolError, match=exp_err):
                    instance.sql(query)
            else:
                instance.sql(query)


class PgprotoRunner:
    def __init__(self, cluster: Cluster):
        # Setup pgproto user
        leader = cluster.leader()
        leader.sql("CREATE USER postgres WITH PASSWORD 'Passw0rd'")
        leader.sql("GRANT CREATE TABLE TO postgres", sudo=True)
        leader.sql("GRANT READ TABLE TO postgres", sudo=True)
        leader.sql("GRANT WRITE TABLE TO postgres", sudo=True)
        leader.sql("GRANT DROP TABLE TO postgres", sudo=True)
        leader.sql("GRANT CREATE PROCEDURE TO postgres", sudo=True)
        leader.sql("GRANT DROP PROCEDURE TO postgres", sudo=True)

        # Connect via psycopg
        host, port = leader.pg_host, leader.pg_port
        conn = psycopg.connect(f"postgres://postgres:Passw0rd@{host}:{port}")

        self.cluster = cluster
        self.conn = conn

    def do_execsql(self, query: str, expected: list):
        result = self.conn.execute(query).fetchall()
        result_len = len(result[0]) if len(result) > 0 else 0
        new_expected = list(map(list, zip(*(iter(expected),) * result_len)))
        compare_results(new_expected, result)

    def do_execsql_exact(self, query: str, expected: list):
        result = self.conn.execute(query).fetchall()
        data = [col for row in result for col in row]
        assert data == expected

    def do_explain_sql(self, query: str, expected: list):
        result = self.conn.execute(query)
        explain = [row[0] for row in result]
        assert explain == expected

    def do_catchsql(self, sql: str, expected: str | list):
        queries = [q.strip() for q in sql.split(";") if q.strip()]

        if isinstance(expected, str):
            expected = [expected]
        elif expected is None:
            expected = [None] * len(queries)

        assert len(queries) == len(expected), (
            f"Mismatch: {len(queries)} SQL queries but {len(expected)} expected errors."
        )

        for query, exp_err in zip(queries, expected):
            if exp_err and exp_err != NOT_AN_ERROR:
                with pytest.raises(psycopg.Error, match=exp_err):
                    self.conn.execute(query)
            else:
                self.conn.execute(query)


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


@pytest.mark.parametrize("runner_cls", [PgprotoRunner, IprotoRunner], scope="class")
class ClusterSingleInstance:
    params: list = []

    @pytest.fixture(scope="class")
    def runner(self, runner_cls, cluster_factory):
        cluster = cluster_factory()
        init_cluster(cluster, 1)
        assert len(cluster.instances) == 1
        cluster.wait_balanced()

        yield runner_cls(cluster)

        cluster.kill()

    def test_sql(self, runner, query: str, is_exact: bool, expected: list, error: str):
        if query.lower().startswith("explain") and not error:
            runner.do_explain_sql(query, expected)
        elif expected and not is_exact:
            runner.do_execsql(query, expected)
        elif expected and is_exact:
            runner.do_execsql_exact(query, expected)
        else:
            runner.do_catchsql(query, error)


# TODO: Add instance_count parameter to the class above and remove this class.
# Note that doing this before fixing vshard rebalancing will introduce more flaky tests.
@pytest.mark.parametrize("runner_cls", [PgprotoRunner, IprotoRunner], scope="class")
class ClusterTwoInstances:
    params: list = []

    @pytest.fixture(scope="class")
    def runner(self, runner_cls, cluster_factory):
        cluster = cluster_factory()
        init_cluster(cluster, 2)
        assert len(cluster.instances) == 2
        cluster.wait_balanced()

        yield runner_cls(cluster)

        cluster.kill()

    def test_sql(self, runner, query: str, expected: list, is_exact: bool, error: str):
        if query.lower().startswith("explain") and not error:
            runner.do_explain_sql(query, expected)
        elif expected and not is_exact:
            runner.do_execsql(query, expected)
        elif expected and is_exact:
            runner.do_execsql_exact(query, expected)
        else:
            runner.do_catchsql(query, error)
