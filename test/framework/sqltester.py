from abc import ABC, abstractmethod
from dataclasses import dataclass
import logging
import os
import textwrap
from collections import Counter
from pathlib import Path
from typing import Any
import pytest
from _pytest.python import FunctionDefinition, Metafunc
from decimal import Decimal
import re
from conftest import TIMEOUT_SCALE, Cluster, TarantoolError, get_pytest_timeout
import psycopg
import enum


NOT_AN_ERROR = "-"


def init_cluster(cluster: Cluster, instance_count: int, replication_factor: int) -> Cluster:
    cluster.deploy(instance_count=instance_count, init_replication_factor=replication_factor)
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
    protocol: str
    run_query_error: type
    replicaset_counts: tuple[int, ...]
    replication_factors: tuple[int, ...]

    @abstractmethod
    def run_query(self, query: str, params: list[Any] | None = None) -> list:
        pass

    def do_catchsql(self, sql: str, expected: str | list | None, params: list[Any] | None = None):
        queries = parse_queries(sql)
        if params is not None:
            assert len(queries) == 1, "SQL tests with PARAMS must contain a single query"

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
                        self.run_query(query, params)
                else:
                    self.run_query(query, params)

        # TODO: replace None with smth to support `--update-sql-snapshots`
        return None, do_check

    def do_execsql(self, query: str, expected: list, params: list[Any] | None = None):
        result = self.run_query(query, params)
        result_len = len(result[0]) if len(result) > 0 else 0
        new_expected = list(map(list, zip(*(iter(expected),) * result_len)))

        def do_check():
            compare_results(new_expected, result)

        # TODO: replace None with smth to support `--update-sql-snapshots`
        return None, do_check

    def do_execsql_exact(self, query: str, expected: list, params: list[Any] | None = None):
        result = self.run_query(query, params)
        output = [col for row in result for col in row]

        def do_check():
            assert output == expected

        # TODO: replace None with smth to support `--update-sql-snapshots`
        return None, do_check

    def do_explain_sql(self, query: str, expected: list | None, params: list[Any] | None = None):
        result = self.run_query(query, params)
        output = [row[0] for row in result]

        def do_check():
            assert output == expected

        return output, do_check


class IprotoRunner(AbstractRunner):
    protocol = "iproto"
    run_query_error = TarantoolError
    replicaset_counts = (2,)
    replication_factors = (1,)

    def __init__(self, cluster: Cluster):
        super().__init__()
        self.cluster = cluster

    def run_query(self, query: str, params: list[Any] | None = None) -> list:
        params = [] if params is None else params
        result = self.cluster.leader().sql(query, *params)
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
    protocol = "pgproto"
    run_query_error = psycopg.Error
    replicaset_counts = (1, 2)
    replication_factors = (1,)

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

    def run_query(self, query: str, params: list[Any] | None = None) -> list:
        with psycopg.RawCursor(self.conn) as cur:
            if params is not None:
                cur.execute(query, params, prepare=True)
            else:
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


# Parse the body of an EXPLAIN `-- EXPECTED:` block into raw text lines.
#
# Unlike `_parse_line` (used for comma-separated typed literals in do_execsql/PARAMS
# blocks), EXPLAIN output must be compared verbatim: every line is opaque plan/SQL
# text, so numbers, `true`/`false`, `null` etc. must NOT be coerced to typed Python
# values (e.g. `float("  1")` silently succeeds and turns the string "  1" into 1.0,
# which then never matches the string produced by the SQL driver on the next run).
# The only special cases are the `''` marker `--update-sql-snapshots` writes for a
# genuinely empty line, and legacy lines manually wrapped in quotes to survive the
# old coercion logic.
def _parse_explain_body(body: str) -> list[str]:
    result: list[str] = []

    for element in body.split("\n"):
        if element == "":
            continue
        elif element.startswith("'") and element.endswith("'"):
            # If element in single quotes, remove them and add as string
            result.append(element[1:-1])
        else:
            result.append(element)

    return result


@dataclass(frozen=True)
class SqlBlock:
    """
    A single `-- TEST:` block of a `.sql` test file.

    Blocks of one file are steps of a single scenario: they run in order
    against the same cluster and share whatever state they create.
    """

    file: Path
    # Name and line only serve to point at the block in a failure report.
    name: str
    line: int
    query: str
    params: list[Any] | None
    expected: list[Any] | None
    is_exact: bool
    error: list[str] | None
    # Span used by `--update-sql-snapshots` to rewrite it in place.
    span: tuple[int, int] | None
    is_explain: bool
    # Configs this block is skipped on, e.g. `1rsX1`, `pgproto` or `iproto-2rsX1`.
    skipped_for: frozenset[str]


@dataclass(frozen=True)
class SqlHeader:
    test_matrix: frozenset[str] | None
    # `-- XFAIL: reason` / `-- SKIP: reason`.
    marks: tuple


_MARKS = {"XFAIL": pytest.mark.xfail, "SKIP": pytest.mark.skip}

_DIRECTIVE_RE = re.compile(r"^-- (?P<key>TEST-MATRIX|XFAIL|SKIP): *(?P<value>.+?) *$", re.M)

_TEST_MATRIX_LINE_RE = re.compile(r"^-- TEST-MATRIX:[^\n]*\n", re.M)


def parse_header(content: str) -> SqlHeader:
    first_block = content.find("-- TEST:")
    preamble = content if first_block < 0 else content[:first_block]

    test_matrix = None
    marks = []
    for match in _DIRECTIVE_RE.finditer(preamble):
        key, value = match.group("key"), match.group("value")
        if key == "TEST-MATRIX":
            test_matrix = frozenset(name.strip() for name in value.split(","))
        else:
            marks.append(_MARKS[key](reason=value))

    return SqlHeader(test_matrix=test_matrix, marks=tuple(marks))


def parse_file(test_file: Path) -> tuple[SqlHeader, list[SqlBlock]]:
    content = test_file.read_text()
    header = parse_header(content)
    test_pattern = (
        # Test name
        r"-- TEST: (?P<name>[^\n]*)\n"
        # SKIP_FOR (optional): comma-separated configs to skip this block on.
        r"(?:-- SKIP_FOR: *(?P<skip_for>[^\n]*)\n)?"
        # SQL query
        r"-- SQL:\n(?P<query>.*?)\n"
        # PARAMS (optional): bind parameters for a single SQL query.
        r"(?:-- PARAMS:\n(?P<params>.*?)(?=-- (?:EXPECTED|UNORDERED|ERROR):|-- TEST:|\Z))?"
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
    blocks: list[SqlBlock] = []
    matches = re.finditer(test_pattern, content, re.DOTALL)
    for match in matches:
        name = match.group("name").strip()
        assert name, "Test name must be provided"

        query = match.group("query")
        if query.startswith("--"):
            continue
        query = query.strip()
        assert query, "SQL query must be provided"

        line = content.count("\n", 0, match.start("name")) + 1

        bind_params = None
        params_body = match.group("params")
        if params_body is not None:
            queries = parse_queries(query)
            assert len(queries) == 1, f"Test {name}: PARAMS are supported only for a single query"
            bind_params = _parse_line(params_body, None, ",")

        skip_for_body = match.group("skip_for")
        skipped_for = frozenset(tag.strip() for tag in skip_for_body.split(",")) if skip_for_body else frozenset()

        kind = match.group("kind")
        is_explain = query.lower().startswith("explain")

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
            if is_explain:
                expected = _parse_explain_body(match.group("body"))
            else:
                expected = _parse_line(match.group("body"), None, ",")

        blocks.append(
            SqlBlock(
                file=test_file,
                name=name,
                line=line,
                query=query,
                params=bind_params,
                expected=expected,
                is_exact=is_exact,
                error=error,
                span=span,
                is_explain=is_explain,
                skipped_for=skipped_for,
            )
        )

    return header, blocks


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


class SqlTestFailure(AssertionError):
    """A `-- TEST:` block failed. Carries the block's location in its message."""


def _record_snapshot_patch(block: SqlBlock, output: list):
    """Queue the actual output to replace the block's expectation in the file."""
    assert block.span is not None

    def fix_line(s):
        if s == "":
            return "''"
        return str(s)

    file_name = str(block.file)
    TEST_PATCHES.setdefault(file_name, {})[block.span] = "\n".join(fix_line(x) for x in output)


def _run_block(runner: AbstractRunner, block: SqlBlock):
    output = None
    try:
        if block.is_explain and not block.error:
            output, do_check = runner.do_explain_sql(block.query, block.expected, block.params)
        elif block.expected and not block.is_exact:
            output, do_check = runner.do_execsql(block.query, block.expected, block.params)
        elif block.expected and block.is_exact:
            output, do_check = runner.do_execsql_exact(block.query, block.expected, block.params)
        else:
            output, do_check = runner.do_catchsql(block.query, block.error, block.params)

        do_check()

    except AssertionError:
        # Only EXPLAIN blocks produce an output we know how to write back.
        if output is not None and block.span is not None:
            _record_snapshot_patch(block, output)

        # XXX: don't forget to re-raise!
        raise


def _raise_block_failure(block: SqlBlock, exc: BaseException):
    location = f"{block.file.name}:{block.line}: -- TEST: {block.name}"
    query = textwrap.indent(block.query, "    ")

    if isinstance(exc, (AssertionError, pytest.fail.Exception)):
        raise SqlTestFailure(f"{location}\n\n{query}\n\n{exc}") from None

    # Anything else is an unexpected error rather than a mismatch.
    raise SqlTestFailure(f"{location}\n\n{query}\n\n{type(exc).__name__}: {exc}") from exc


RUNNERS: tuple[type[AbstractRunner], ...] = (PgprotoRunner, IprotoRunner)

SECONDS_PER_BLOCK = 1.0


@dataclass(frozen=True)
class SqlTestSpec:
    """What one collected item runs: a protocol, a cluster topology and the blocks."""

    runner_cls: type[AbstractRunner]
    replicaset_count: int
    replication_factor: int
    blocks: list[SqlBlock]

    @property
    def name(self) -> str:
        return f"{self.runner_cls.protocol}-{self.topology}"

    @property
    def topology(self) -> str:
        return f"{self.replicaset_count}rsX{self.replication_factor}"

    @property
    def instance_count(self) -> int:
        return self.replicaset_count * self.replication_factor

    @property
    def tags(self) -> frozenset[str]:
        return frozenset({self.runner_cls.protocol, self.topology, self.name})


def all_specs(blocks: list[SqlBlock]) -> list[SqlTestSpec]:
    return [
        SqlTestSpec(runner_cls, count, factor, blocks)
        for runner_cls in RUNNERS
        for count in runner_cls.replicaset_counts
        for factor in runner_cls.replication_factors
    ]


def format_cluster_table(specs: list[SqlTestSpec]) -> str:
    header = ("CONFIG", "PROTOCOL", "REPLICASETS", "REPLICATION FACTOR", "INSTANCES")
    rows = [
        (
            spec.name,
            spec.runner_cls.protocol,
            str(spec.replicaset_count),
            str(spec.replication_factor),
            str(spec.instance_count),
        )
        for spec in specs
    ]
    widths = [max(len(cell) for cell in column) for column in zip(header, *rows)]

    def format_row(cells: tuple[str, ...]) -> str:
        cells = tuple(
            cell.ljust(width) if i < 2 else cell.rjust(width) for i, (cell, width) in enumerate(zip(cells, widths))
        )
        return "  ".join(cells).rstrip()

    separator = tuple("-" * width for width in widths)
    return "\n".join(format_row(row) for row in (header, separator, *rows))


@pytest.fixture
def sql_runner(request, cluster_factory):
    """Deploy the cluster an item's spec asks for and wrap it in its runner."""
    spec: SqlTestSpec = request.node.sql_spec

    cluster = cluster_factory()
    init_cluster(cluster, spec.instance_count, spec.replication_factor)
    assert len(cluster.instances) == spec.instance_count
    cluster.wait_until_buckets_balanced()

    yield spec.runner_cls(cluster)

    # Speed up graceful shutdown so teardown doesn't hit the default 30s wait.
    cluster.terminate(on_shutdown_timeout=1)


def run_sql_file(sql_runner, request):
    """
    Run every `-- TEST:` block of a `.sql` file, in order.
    """
    spec: SqlTestSpec = request.node.sql_spec

    # `--update-sql-snapshots` is meant to refresh every stale EXPLAIN in one
    # pass, so under it we keep going and only report the first failure at the end.
    update_snapshots = request.config.getoption("update_sql_snapshots")

    failure: tuple[SqlBlock, BaseException] | None = None
    for block in spec.blocks:
        if block.skipped_for & spec.tags:
            logging.info(f"{block.file.name}:{block.line}: -- TEST: {block.name} skipped on {spec.name}")
            continue

        try:
            _run_block(sql_runner, block)
        except (Exception, pytest.fail.Exception) as exc:
            if failure is None:
                failure = (block, exc)
            if not (update_snapshots and isinstance(exc, AssertionError)):
                break

    if failure is not None:
        _raise_block_failure(*failure)


class SqlFile(pytest.File):
    """
    Collector turning one `.sql` file into one test per protocol and cluster size.
    """

    def collect(self):
        header, blocks = parse_file(self.path)
        assert blocks, f"{self.path} defines no `-- TEST:` blocks"

        specs = self._select_specs(header, blocks)
        self._check_skip_for_tags(blocks, specs)
        needs_one_worker = self._needs_one_worker()

        for spec in specs:
            for item in self._make_items(spec.name):
                item.sql_spec = spec

                if needs_one_worker:
                    item.add_marker(pytest.mark.xdist_group(name=self.path.name))
                self._add_timeout_marker(item, len(blocks))
                for mark in header.marks:
                    item.add_marker(mark)

                yield item

    def _make_items(self, name: str):
        """
        Build the item(s) for one spec, using `pytest_generate_tests`.
        """
        definition = FunctionDefinition.from_parent(self, name=name, callobj=run_sql_file)
        fixtureinfo = definition._fixtureinfo
        metafunc = Metafunc(
            definition=definition,
            fixtureinfo=fixtureinfo,
            config=self.config,
            cls=None,
            module=None,
            _ispytest=True,
        )
        self.ihook.pytest_generate_tests(metafunc=metafunc)

        # No plugin asked for extra parametrization.
        if not metafunc._calls:
            yield pytest.Function.from_parent(self, name=name, fixtureinfo=fixtureinfo, callobj=run_sql_file)
            return

        fixtureinfo.prune_dependency_tree()
        for callspec in metafunc._calls:
            yield pytest.Function.from_parent(
                self,
                name=f"{name}[{callspec.id}]",
                callspec=callspec,
                fixtureinfo=fixtureinfo,
                keywords={callspec.id: True},
                originalname=name,
                callobj=run_sql_file,
            )

    def _needs_one_worker(self) -> bool:
        """
        Whether this file's items must all land on the same xdist worker.
        """
        return bool(self.config.getoption("update_sql_snapshots"))

    def _select_specs(self, header: SqlHeader, blocks: list[SqlBlock]) -> list[SqlTestSpec]:
        """
        The specs the file's mandatory `-- TEST-MATRIX:` header asks for.
        """
        specs = all_specs(blocks)
        known = [spec.name for spec in specs]

        if self.config.getoption("update_sql_test_matrix"):
            self._write_test_matrix_header(specs)
            return specs

        if header.test_matrix is None:
            raise ValueError(f"{self.path}: missing `-- TEST-MATRIX:` header, pick from {known}")

        unknown = sorted(header.test_matrix - set(known))
        if unknown:
            raise ValueError(f"{self.path}: -- TEST-MATRIX: unknown config(s) {unknown}, known: {known}")

        return [spec for spec in specs if spec.name in header.test_matrix]

    def _write_test_matrix_header(self, specs: list[SqlTestSpec]):
        """
        Rewrite the file's `-- TEST-MATRIX:` header to `specs`, adding it if missing.
        """
        content = self.path.read_text()
        line = f"-- TEST-MATRIX: {', '.join(spec.name for spec in specs)}\n"

        first_block = content.find("-- TEST:")
        preamble = content if first_block < 0 else content[:first_block]
        match = _TEST_MATRIX_LINE_RE.search(preamble)
        if match:
            patched = content[: match.start()] + line + content[match.end() :]
        else:
            patched = line + "\n" + content

        if patched == content:
            return

        logging.warning(f"updating test matrix header of {self.path}")

        # Every xdist worker collects the file, so the write has to be atomic.
        tmp = self.path.with_suffix(f".sql.{os.getpid()}.tmp")
        tmp.write_text(patched)
        tmp.replace(self.path)

    def _check_skip_for_tags(self, blocks: list[SqlBlock], specs: list[SqlTestSpec]):
        known = frozenset().union(*(spec.tags for spec in specs))
        for block in blocks:
            unknown = sorted(block.skipped_for - known)
            if unknown:
                raise ValueError(
                    f"{self.path}:{block.line}: -- SKIP_FOR: unknown config(s) {unknown}, known: {sorted(known)}"
                )

    def _add_timeout_marker(self, item: pytest.Item, block_count: int):
        # An explicit --timeout must keep winning over what we compute here.
        if self.config.getoption("timeout") is not None:
            return

        base = get_pytest_timeout(self.config)
        per_block = SECONDS_PER_BLOCK * block_count * TIMEOUT_SCALE
        item.add_marker(pytest.mark.timeout(base + per_block))


def pytest_cmdline_main(config):
    """
    `--list-sql-test-matrix` prints the configs and exits without running anything.
    """
    if not config.getoption("list_sql_test_matrix"):
        return None

    print(format_cluster_table(all_specs([])))
    return 0


def pytest_collect_file(file_path: Path, parent):
    """
    Collect a `.sql` file as a test, provided its directory registers one.
    """
    if file_path.suffix != ".sql" or file_path.parent.name != "sql":
        return None

    return SqlFile.from_parent(parent, path=file_path)
