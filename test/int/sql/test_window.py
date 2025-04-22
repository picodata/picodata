from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("window-1.sql")
class TestWindow1(ClusterTwoInstances):
    pass


@sql_test_file("window-2.sql")
class TestWindow2(ClusterTwoInstances):
    pass


@sql_test_file("window-3.sql")
class TestWindow3(ClusterTwoInstances):
    pass


@sql_test_file("window-4.sql")
class TestWindow4(ClusterTwoInstances):
    pass


@sql_test_file("window-5.sql")
class TestWindow5(ClusterTwoInstances):
    pass


@sql_test_file("window-6.sql")
class TestWindow6(ClusterTwoInstances):
    pass


@sql_test_file("window-7.sql")
class TestWindow7(ClusterTwoInstances):
    pass


@sql_test_file("window-8.sql")
class TestWindow8(ClusterTwoInstances):
    pass


@sql_test_file("window-9.sql")
class TestWindow9(ClusterTwoInstances):
    pass


@sql_test_file("window-10.sql")
class TestWindow10(ClusterTwoInstances):
    pass


@sql_test_file("window-11.sql")
class TestWindow11(ClusterTwoInstances):
    pass


@sql_test_file("window-12.sql")
class TestWindow12(ClusterTwoInstances):
    pass
