from abc import ABC, abstractmethod
import inspect
import logging
from collections import Counter
from pathlib import Path
from typing import Type
from typing import Any
import pytest
from decimal import Decimal
import re
from conftest import Cluster, TarantoolError
import psycopg
import enum


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


# Extract queries from string divided by `;`.
#
# For the following input 3 queries will be returned:
# ```sql
# SELECT ';';
# SELECT 1;
# SELECT "kek;";
# ```
def parse_queries(raw_queries: str):
    class State(enum.Enum):
        SEARCHING_FOR_DELIMITER = enum.auto()
        PARSING_STRING_LITERAL = enum.auto()
        PARSING_QUOTED_IDENTIFIER = enum.auto()
        PARSING_ESCAPED_QUOTE = enum.auto()
        STARING_PARSING_BLOCK_BODY = enum.auto()
        PARSING_BLOCK_BODY = enum.auto()
        FINISHING_BLOCK_BODY = enum.auto()

    delimiter = ";"
    state = State.SEARCHING_FOR_DELIMITER
    cur_start = 0

    assert raw_queries[-1] == delimiter, f"All queries must be terminated with '{delimiter}'"

    queries = []
    for i, ch in enumerate(raw_queries):
        match state:
            case State.SEARCHING_FOR_DELIMITER:
                if ch == delimiter:
                    queries.append(raw_queries[cur_start : i + 1])
                    cur_start = i + 1
                elif ch == "'":
                    state = State.PARSING_STRING_LITERAL
                elif ch == '"':
                    state = State.PARSING_QUOTED_IDENTIFIER
                elif ch == "$" and raw_queries[i + 1] == "$":
                    state = State.STARING_PARSING_BLOCK_BODY

            case State.PARSING_STRING_LITERAL:
                if ch == "'":
                    if raw_queries[i + 1] == "'":
                        state = State.PARSING_ESCAPED_QUOTE
                    else:
                        state = State.SEARCHING_FOR_DELIMITER

            case State.PARSING_QUOTED_IDENTIFIER:
                if ch == '"':
                    state = State.SEARCHING_FOR_DELIMITER

            case State.PARSING_ESCAPED_QUOTE:
                assert ch == "'"
                state = State.SEARCHING_FOR_DELIMITER

            case State.STARING_PARSING_BLOCK_BODY:
                assert ch == "$"
                state = State.PARSING_BLOCK_BODY

            case State.PARSING_BLOCK_BODY:
                if ch == "$" and raw_queries[i + 1] == "$":
                    state = State.FINISHING_BLOCK_BODY

            case State.FINISHING_BLOCK_BODY:
                assert ch == "$"
                state = State.SEARCHING_FOR_DELIMITER

    assert state == State.SEARCHING_FOR_DELIMITER
    assert cur_start == len(raw_queries), "Couldn't parse all queries"

    queries = [q.lstrip() for q in queries]

    return queries


class AbstractRunner(ABC):
    run_query_error: type

    @abstractmethod
    def run_query(self, query: str) -> list:
        pass

    def do_catchsql(self, sql: str, expected: str | list):
        queries = parse_queries(sql)

        if isinstance(expected, str):
            expected = [expected]
        elif expected is None:
            expected = [None] * len(queries)

        assert len(queries) == len(expected), (
            f"Mismatch: {len(queries)} SQL queries but {len(expected)} expected errors."
        )

        def do_check():
            for query, exp_err in zip(queries, expected):
                if exp_err and exp_err != NOT_AN_ERROR:
                    with pytest.raises(self.run_query_error, match=exp_err):
                        self.run_query(query)
                else:
                    self.run_query(query)

        # TODO: replace None with smth to support `--update-sql-snapshots`
        return None, do_check

    def do_execsql(self, query: str, expected: list):
        result = self.run_query(query)
        result_len = len(result[0]) if len(result) > 0 else 0
        new_expected = list(map(list, zip(*(iter(expected),) * result_len)))

        def do_check():
            compare_results(new_expected, result)

        # TODO: replace None with smth to support `--update-sql-snapshots`
        return None, do_check

    def do_execsql_exact(self, query: str, expected: list):
        result = self.run_query(query)
        output = [col for row in result for col in row]

        def do_check():
            assert output == expected

        # TODO: replace None with smth to support `--update-sql-snapshots`
        return None, do_check

    def do_explain_sql(self, query: str, expected: list):
        result = self.run_query(query)
        output = [row[0] for row in result]

        def do_check():
            assert output == expected

        return output, do_check


class IprotoRunner(AbstractRunner):
    run_query_error = TarantoolError

    def __init__(self, cluster: Cluster):
        super().__init__()
        self.cluster = cluster

    def run_query(self, query: str) -> list:
        result = self.cluster.leader().sql(query)
        rows: list[Any] = []
        for row in result:
            match row:
                case list():
                    rows.append(row)
                case tuple():
                    rows.append(row)
                case _:
                    # This chicanery is needed for EXPLAIN.
                    rows.append((row,))
        return rows


class PgprotoRunner(AbstractRunner):
    run_query_error = psycopg.Error

    def __init__(self, cluster: Cluster):
        super().__init__()

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

    def run_query(self, query: str) -> list:
        with self.conn.cursor() as cur:
            cur.execute(query)
            # https://www.psycopg.org/psycopg3/docs/api/cursors.html#psycopg.Cursor.rownumber
            if cur.rownumber is None:
                return []
            return cur.fetchall()


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
        r"-- TEST: (?P<name>[^\n]*)\n"
        # SQL query
        r"-- SQL:\n(?P<query>.*?)\n"
        # EXPECTED (optional): tuples returned by the picodata must
        # appear in exactly the same order as specified in the test
        # result.
        # UNORDERED (optional): tuples returned by the picodata have
        # no ordering guarantees. Expected and actual values will be
        # sorted before comparison.
        # ERROR (optional): error message that must be raised by the
        # picodata.
        r"(?:-- (?P<kind>EXPECTED|UNORDERED|ERROR):\n(?P<body>.*?))?"
        # Next test or end of file
        r"(?=-- TEST:|\Z)"
    )
    params = []
    matches = re.finditer(test_pattern, content, re.DOTALL)
    for match in matches:
        name = match.group("name").strip()
        assert name, "Test name must be provided"

        query = match.group("query")
        if query.startswith("--"):
            continue
        query = query.strip()
        assert query, "SQL query must be provided"

        kind = match.group("kind")

        span = None
        error = None
        expected = None
        is_exact: bool = False
        if kind == "ERROR":
            span = match.span("body")
            error = match.group("body").strip().split("\n")
        elif kind == "EXPECTED" or kind == "UNORDERED":
            span = match.span("body")
            is_exact = kind == "EXPECTED"
            if query.lower().startswith("explain"):
                expected = _parse_line(match.group("body"), "\n", "\n")
            else:
                expected = _parse_line(match.group("body"), None, ",")

        params.append(pytest.param(query, expected, is_exact, error, test_file, span, id=name))

    return params


def sql_test_file(file_name: str):
    def inner(cls):
        cls = pytest.mark.xdist_group(name=f"{file_name}#{cls.__name__}")(cls)
        cls.params = parse_file(cls, file_name)
        return cls

    return inner


TEST_SQL_ARGS = ["query", "expected", "is_exact", "error", "file_name", "span"]


# Pytest hook to generate tests from parameters. For details see test/framework/sqltester.py
def pytest_generate_tests(metafunc):
    if metafunc.function.__name__ == "test_sql":
        assert "query" in metafunc.fixturenames
        assert "expected" in metafunc.fixturenames
        assert "error" in metafunc.fixturenames
        metafunc.parametrize(TEST_SQL_ARGS, metafunc.cls.params)


TEST_PATCHES: dict[str, dict[tuple[int, int], str]] = dict()


@pytest.fixture(scope="session", autouse=True)
def patch_sql_snapshots(pytestconfig):
    yield  # wait until all tests have finished

    if not pytestconfig.getoption("update_sql_snapshots"):
        return

    for file, patches in TEST_PATCHES.items():
        logging.warning(f"patching file {file}")
        with open(file) as f:
            content = f.read()

        spans = list(patches.keys())
        spans.sort(reverse=True)

        for start, end in spans:
            old_snapshot = content[start:end]
            old_trailing_newlines = len(old_snapshot) - len(old_snapshot.rstrip("\n"))

            new_snapshot = patches[(start, end)]
            new_trailing_newlines = len(new_snapshot) - len(new_snapshot.rstrip("\n"))

            # The current test ends where the next one starts, so we have
            # to adjust its tail to account for newlines we'd otherwise lose.
            trailing_newlines = max(2, old_trailing_newlines, new_trailing_newlines)
            new_snapshot = new_snapshot.rstrip("\n") + "\n" * trailing_newlines
            content = content[:start] + new_snapshot + content[end:]

        content = content.rstrip("\n") + "\n"
        with open(file, "w") as f:
            f.write(content)


class AbstractCluster(ABC):
    instance_count: int

    @pytest.fixture(scope="class")
    def runner(self, runner_cls, cluster_factory):
        cluster = cluster_factory()
        init_cluster(cluster, self.instance_count)
        assert len(cluster.instances) == self.instance_count
        cluster.wait_until_buckets_balanced()

        yield runner_cls(cluster)

        cluster.kill()

    def test_sql(
        self,
        runner,
        query: str,
        is_exact: bool,
        expected: list,
        error: str,
        file_name: str,
        span: tuple[int, int],
    ):
        """
        See `TEST_SQL_ARGS` above!
        """

        output = None
        try:
            if query.lower().startswith("explain") and not error:
                output, do_check = runner.do_explain_sql(query, expected)
            elif expected and not is_exact:
                output, do_check = runner.do_execsql(query, expected)
            elif expected and is_exact:
                output, do_check = runner.do_execsql_exact(query, expected)
            else:
                output, do_check = runner.do_catchsql(query, error)

            do_check()

        except AssertionError:
            if output is not None:

                def fix_line(s):
                    if s == "":
                        return "''"
                    return str(s)

                output = "\n".join(fix_line(x) for x in output)

                # Now, register the test to be updated.
                file_name = str(file_name)
                global TEST_PATCHES
                if file_name not in TEST_PATCHES:
                    TEST_PATCHES[file_name] = {}
                TEST_PATCHES[file_name][span] = output

            # XXX: don't forget to re-raise!
            raise


@pytest.mark.parametrize("runner_cls", [PgprotoRunner, IprotoRunner], scope="class")
class ClusterSingleInstance(AbstractCluster):
    params: list = []
    instance_count: int = 1


# TODO: Add instance_count parameter to the class above and remove this class.
# Note that doing this before fixing vshard rebalancing will introduce more flaky tests.
@pytest.mark.parametrize("runner_cls", [PgprotoRunner, IprotoRunner], scope="class")
class ClusterTwoInstances(AbstractCluster):
    params: list = []
    instance_count: int = 2
