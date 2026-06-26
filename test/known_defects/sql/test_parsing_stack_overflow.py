import pytest
from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("parsing_stack_overflow.sql")
@pytest.mark.xfail(reason="panic https://git.picodata.io/core/picodata/-/issues/2992")
class TestParsingStackOverflow(ClusterSingleInstance):
    pass
