from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("integer_ranges.sql")
class TestIntegerRanges(ClusterTwoInstances):
    pass
