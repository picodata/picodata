from sqltester import (  # noqa F401
    ClusterTwoInstances,
    cluster_2,
    parse_file,
    pytest_generate_tests,
)


class TestExample1(ClusterTwoInstances):
    params = parse_file("example.txt")


class TestExample2(ClusterTwoInstances):
    params = parse_file("example.txt")
