import os
import pytest
import re
from conftest import Cluster, TarantoolError


def init_cluster(cluster: Cluster, instance_count: int) -> Cluster:
    cluster.deploy(instance_count=instance_count)
    for i in cluster.instances:
        i.wait_online()
    return cluster


def do_execsql(cluster: Cluster, query: str, expected: list):
    instance = cluster.leader()
    result = instance.sql(query)
    data = [col for row in result for col in row]
    assert data == expected


def do_catchsql(cluster: Cluster, sql: str, expected: str):
    instance = cluster.leader()
    if expected:
        msg = re.escape(expected.strip())
        with pytest.raises(TarantoolError, match=msg):
            instance.sql(sql)
    else:
        # Split SQL by semicolon and execute each query separately
        for query in sql.split(";"):
            instance.sql(query)


def _parse_line(input_string):
    result = []
    elements = input_string.split(",")  # Split line by comma

    for element in elements:
        element = element.strip()  # Remove leading and trailing whitespaces

        if element.startswith("'") and element.endswith("'"):
            # If element in single quotes, remove them and add as string
            result.append(element[1:-1])
        elif element.lower() in {"null", "none", "nil", ""}:
            # If element is a null, add as None
            result.append(None)
        elif element.isdigit() or (element[0] == "-" and element[1:].isdigit()):
            # If element is a number, add as int
            result.append(int(element))
        else:
            try:
                # Try to convert element to float
                result.append(float(element))
            except ValueError:
                # If not possible, add as string
                result.append(element)

    return result


def parse_file(file_name: str) -> list:
    current_file_path = os.path.abspath(__file__)
    dir_path = os.path.dirname(current_file_path)
    path = os.path.join(dir_path, file_name)
    with open(path, "r") as f:
        content = f.read()
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
        query = match[1].strip()
        assert query, "SQL query must be provided"
        expected = _parse_line(match[2]) if match[2] else None
        error = match[3].strip() if match[3] else None
        assert not (expected and error), "Cannot provide both expected result and error"
        params.append(pytest.param(query, expected, error, id=name))
    return params


# Pytest hook to generate tests from parameters
def pytest_generate_tests(metafunc):
    if metafunc.function.__name__ == "test_sql":
        assert "query" in metafunc.fixturenames
        assert "expected" in metafunc.fixturenames
        assert "error" in metafunc.fixturenames
        metafunc.parametrize(["query", "expected", "error"], metafunc.cls.params)


@pytest.fixture(scope="class")
def cluster_2(cluster):
    yield init_cluster(cluster, 2)
    cluster.kill()


class ClusterTwoInstances:
    params: list = []

    def test_sql(self, cluster_2: Cluster, query: str, expected: list, error: str):
        if expected:
            do_execsql(cluster_2, query, expected)
        else:
            do_catchsql(cluster_2, query, error)
