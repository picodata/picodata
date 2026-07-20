import pytest
from conftest import Cluster, Instance, TarantoolError, log_crawler

# Tests the implementation of `ALTER SYSTEM SET LOCAL` / `ALTER SYSTEM RESET LOCAL`
# and the `pico_log_level` / `pico_log_level_list` SQL functions.
#
# `ALTER SYSTEM ... LOCAL` is a fully local operation: it affects only
# the instance the query is dispatched to and is not persisted (the value is
# reset to the configured default on restart).

# Numerical values of tarantool log levels, see `tarantool::log::SayLevel`.
SAY_FATAL = 0
SAY_SYSTEM = 1
SAY_ERROR = 2
SAY_CRIT = 3
SAY_WARN = 4
SAY_INFO = 5
SAY_VERBOSE = 6
SAY_DEBUG = 7

# The configured log level `deploy_cluster_with_configured_log_level` will start the instances with.
DEFAULT_LOG_LEVEL = "verbose"

# All log levels and their numerical representations, as returned by
# `pico_log_level_map()`.
_ALL_LOG_LEVELS = {"error": 2, "warn": 4, "info": 5, "system": 1, "debug": 7, "verbose": 6, "fatal": 0, "crit": 3}


def get_log_level_sql(instance: Instance) -> str:
    [[level]] = instance.sql("SELECT pico_log_level()")
    return level


def get_log_level_lua(instance: Instance) -> int:
    return instance.eval("return box.cfg.log_level")


def set_log_level_sql(instance: Instance, value: str) -> None:
    instance.sql(f"ALTER SYSTEM SET LOCAL log_level = '{value}'")


def deploy_cluster_with_configured_log_level(cluster: Cluster, instance_count: int) -> list[Instance]:
    instances = cluster.deploy(instance_count=instance_count, wait_online=False)

    for instance in instances:
        # configure the log level through env to override logic in Instance.start
        instance.env["PICODATA_LOG_LEVEL"] = DEFAULT_LOG_LEVEL

    cluster.wait_online()
    cluster.wait_until_buckets_balanced()

    return instances


def test_alter_system_local_set_log_level(cluster: Cluster):
    (instance,) = deploy_cluster_with_configured_log_level(cluster, instance_count=1)

    # The default log level in the test environment is `verbose`.
    assert get_log_level_sql(instance) == DEFAULT_LOG_LEVEL
    assert get_log_level_lua(instance) == SAY_VERBOSE

    # Change the log level using the `=` syntax.
    instance.sql("ALTER SYSTEM SET LOCAL log_level = 'debug'")
    assert get_log_level_sql(instance) == "debug"
    assert get_log_level_lua(instance) == SAY_DEBUG

    # Change the log level using the `TO` syntax.
    instance.sql("ALTER SYSTEM SET LOCAL log_level TO 'info'")
    assert get_log_level_sql(instance) == "info"
    assert get_log_level_lua(instance) == SAY_INFO

    # A quoted identifier should also work as a parameter name.
    instance.sql("ALTER SYSTEM SET LOCAL \"log_level\" = 'warn'")
    assert get_log_level_sql(instance) == "warn"
    assert get_log_level_lua(instance) == SAY_WARN

    # Resetting the log level restores the configured default.
    instance.sql("ALTER SYSTEM RESET LOCAL log_level")
    assert get_log_level_sql(instance) == DEFAULT_LOG_LEVEL
    assert get_log_level_lua(instance) == SAY_VERBOSE

    # `SET ... TO DEFAULT` is equivalent to `RESET`.
    set_log_level_sql(instance, "debug")
    instance.sql("ALTER SYSTEM SET LOCAL log_level TO DEFAULT")
    assert get_log_level_sql(instance) == DEFAULT_LOG_LEVEL
    assert get_log_level_lua(instance) == SAY_VERBOSE

    # `SET ... = DEFAULT` is also equivalent to `RESET`.
    set_log_level_sql(instance, "debug")
    instance.sql("ALTER SYSTEM SET LOCAL log_level = DEFAULT")
    assert get_log_level_sql(instance) == DEFAULT_LOG_LEVEL
    assert get_log_level_lua(instance) == SAY_VERBOSE


def test_alter_system_local_log_level_invalid_values(cluster: Cluster):
    (instance,) = deploy_cluster_with_configured_log_level(cluster, instance_count=1)

    # Unknown parameter.
    with pytest.raises(
        TarantoolError,
        match=r"unknown parameter: 'non_existing'",
    ):
        instance.sql("ALTER SYSTEM SET LOCAL non_existing = 'debug'")

    with pytest.raises(
        TarantoolError,
        match=r"unknown parameter: 'non_existing'",
    ):
        instance.sql("ALTER SYSTEM RESET LOCAL non_existing")

    # The log level is expected to be a string.
    with pytest.raises(
        TarantoolError,
        match=r"invalid value for 'log_level': expected string, got integer",
    ):
        instance.sql("ALTER SYSTEM SET LOCAL log_level = 1")

    with pytest.raises(
        TarantoolError,
        match=r"invalid value for 'log_level': expected string, got boolean",
    ):
        instance.sql("ALTER SYSTEM SET LOCAL log_level = true")

    # A non-string literal that can't be cast to a string.
    with pytest.raises(
        TarantoolError,
        match=r"invalid value for 'log_level': expected string, got decimal",
    ):
        instance.sql("ALTER SYSTEM SET LOCAL log_level = 1.5")

    # Unknown log level variant. Note: the error message contains both
    # single and double quotes (around the value and the list of valid
    # levels), so we match on a distinctive substring without quotes.
    with pytest.raises(
        TarantoolError,
        match=r"unknown variant \"not_a_level\" of enum `LogLevel`",
    ):
        instance.sql("ALTER SYSTEM SET LOCAL log_level = 'not_a_level'")

    # NULL is not a valid log level.
    with pytest.raises(
        TarantoolError,
        match=r"invalid value for 'log_level': expected string, got null",
    ):
        instance.sql("ALTER SYSTEM SET LOCAL log_level = NULL")

    # None of the failing statements above changed the current level.
    assert get_log_level_sql(instance) == DEFAULT_LOG_LEVEL

    # `ALTER SYSTEM RESET LOCAL ALL` is not supported yet.
    with pytest.raises(
        TarantoolError,
        match=r"unsupported action/entity: ALTER SYSTEM RESET LOCAL ALL",
    ):
        instance.sql("ALTER SYSTEM RESET LOCAL ALL")


def test_list_log_levels(cluster: Cluster):
    (instance,) = deploy_cluster_with_configured_log_level(cluster, instance_count=1)

    [[levels]] = instance.sql("SELECT pico_log_level_map()")
    assert levels == _ALL_LOG_LEVELS

    # The current log level must be one of the listed levels.
    assert get_log_level_sql(instance) in levels.keys()

    [[level_int]] = instance.sql("SELECT pico_log_level_map()[pico_log_level()]")
    assert type(level_int) is int


def test_alter_system_local_is_not_persistent(cluster: Cluster):
    (instance,) = deploy_cluster_with_configured_log_level(cluster, instance_count=1)

    # Sanity check: default level.
    assert get_log_level_sql(instance) == DEFAULT_LOG_LEVEL

    # Change the level dynamically.
    set_log_level_sql(instance, "debug")
    assert get_log_level_sql(instance) == "debug"

    # Restart the instance; the dynamically set value is gone and the
    # configured default is restored.
    instance.restart()
    instance.wait_online()
    assert get_log_level_sql(instance) == DEFAULT_LOG_LEVEL


def test_alter_system_local_is_local_to_instance(cluster: Cluster):
    i1, i2 = deploy_cluster_with_configured_log_level(cluster, instance_count=2)

    assert get_log_level_sql(i1) == DEFAULT_LOG_LEVEL
    assert get_log_level_sql(i2) == DEFAULT_LOG_LEVEL

    # Changing the level on i1 must not affect i2.
    set_log_level_sql(i1, "debug")
    assert get_log_level_sql(i1) == "debug"
    assert get_log_level_sql(i2) == DEFAULT_LOG_LEVEL

    # And vice versa.
    set_log_level_sql(i2, "info")
    assert get_log_level_sql(i1) == "debug"
    assert get_log_level_sql(i2) == "info"


def test_alter_system_local_access_denied(cluster: Cluster):
    (instance,) = deploy_cluster_with_configured_log_level(cluster, instance_count=1)

    instance.sql("CREATE USER alice WITH PASSWORD 'T0psecret' USING chap-sha1")

    # A non-admin user is not allowed to run ALTER SYSTEM ... LOCAL.
    with pytest.raises(
        TarantoolError,
        match=r"ALTER SYSTEM LOCAL access is denied for user 'alice",
    ):
        instance.sql("ALTER SYSTEM SET LOCAL log_level = 'debug'", user="alice", password="T0psecret")

    with pytest.raises(
        TarantoolError,
        match=r"ALTER SYSTEM LOCAL access is denied for user 'alice",
    ):
        instance.sql("ALTER SYSTEM RESET LOCAL log_level", user="alice", password="T0psecret")

    # The inspection functions are available to everyone.
    [[level]] = instance.sql("SELECT pico_log_level()", user="alice", password="T0psecret")
    assert level == DEFAULT_LOG_LEVEL
    [[levels]] = instance.sql("SELECT pico_log_level_map()", user="alice", password="T0psecret")
    assert levels == _ALL_LOG_LEVELS


def test_alter_system_local_affects_logging(cluster: Cluster):
    (instance,) = deploy_cluster_with_configured_log_level(cluster, instance_count=1)

    # The `set_log_level` implementation emits an info-level log message
    # `dynamically changed log_level to <Level>` *after* applying the new
    # level, so the message is emitted at the newly active level.

    # Bring the level down to `fatal`. The corresponding message is emitted
    # at the new (fatal) level, so it must not appear.
    lc_must_not_appear = log_crawler(instance, "dynamically changed log_level to Fatal")
    set_log_level_sql(instance, "fatal")
    assert get_log_level_sql(instance) == "fatal"

    # Now any info-level message should be suppressed (since fatal < info).
    # Setting the level back to `debug` emits an info log at the new (debug)
    # level, so it is visible -- use it as a barrier.
    lc_barrier = log_crawler(instance, "dynamically changed log_level to Debug")
    set_log_level_sql(instance, "debug")
    lc_barrier.wait_matched()

    # If the suppressed message didn't appear by the time the barrier did, it
    # never will.
    assert not lc_must_not_appear.matched

    # Verify that the change also affects lua logging via `require('log')`.
    # Set level to fatal so that info-level lua messages are suppressed.
    set_log_level_sql(instance, "fatal")
    assert get_log_level_sql(instance) == "fatal"

    # An info-level lua log message must not appear.
    lc_lua_must_not_appear = log_crawler(instance, "lua_info_must_not_appear")
    instance.eval("require('log').info('lua_info_must_not_appear')")

    # Set level to verbose so info messages are visible again.
    set_log_level_sql(instance, "verbose")
    lc_lua_barrier = log_crawler(instance, "lua_info_barrier")
    instance.eval("require('log').info('lua_info_barrier')")
    lc_lua_barrier.wait_matched()

    # If the suppressed message didn't appear by the time the barrier did, it
    # never will.
    assert not lc_lua_must_not_appear.matched
