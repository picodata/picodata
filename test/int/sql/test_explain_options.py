from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


# Tests various combinations of explain facets
@sql_test_file("explain_options.sql")
class TestExplainOptions(ClusterSingleInstance):
    pass
