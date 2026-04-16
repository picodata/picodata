from conftest import Cluster
from conftest import Instance
from conftest import log_crawler
from conftest import ProcessDead
from conftest import TarantoolError
from conftest import Retriable
from dataclasses import dataclass
from dataclasses import field
from framework.util import ExpectedError

import json
import requests


@dataclass
class HealthCheck:
    """
    Health check that verifies distributed operations across a cluster.

    Picks random instances (excluding those in `exclude`) to perform each step,
    ensuring that DDL, sharded DML, cross-table queries, truncation, and cleanup
    all work correctly through the Raft consensus. Requires at least 3 alive nodes.
    """

    cluster: Cluster

    exclude: list[Instance] = field(default_factory=list)
    """
    Instances that would be ignored when picking a random one to perform
    health check. Currently, the only use case for this field is to avoid
    dead instances participate in the check (which will raise exceptions).
    """

    crawlers: dict[Instance, log_crawler | None] = field(default_factory=dict)
    """
    Log crawlers for all instances in a cluster that are not excluded.
    Used to handle specific log-only errors, such as version mismatch on join.
    """

    error: ExpectedError | None = None
    """
    If `error` is `None`, the health check is expected to pass cleanly, otherwise
    is expected to fail with the specified error. See `ExpectedError` for details.
    """

    homogeneous: bool = True
    """
    When `exclude` list is not empty, `error` is set, and this field is not set,
    the error "DDL in heterogeneous cluster is prohibited" is ignored, because
    otherwise it won't be possible to verify SQL queries in the non-full cluster.
    """

    def __post_init__(self) -> None:
        if self.error is None or not self.error.expects_log:
            return

        for instance in self.cluster.instances:
            if instance not in self.exclude:
                pattern = self.error.log_pattern
                assert pattern is not None
                crawler = log_crawler(instance, pattern)
                self.crawlers[instance] = crawler

    def perform(self) -> None:
        try:
            self._wait_governor_finish_activities()
            self._ensure_available_nodes_are_online()
            self._wait_cluster_buckets_are_balanced()
            self._verify_common_distributed_operations()
        except ProcessDead as exc:
            handle_process_dead(exc, self)
        except TarantoolError as exc:
            handle_tarantool_error(exc, self)
        except Exception as exc:
            handle_any_exception(exc, self)

    def _ensure_available_nodes_are_online(self) -> None:
        version = self.cluster.version()

        for instance in self.cluster.instances:
            if instance in self.exclude:
                continue

            # HTTP health endpoints were introduced in version 26 by major.
            # See <https://git.picodata.io/core/picodata/merge_requests/2834>.
            if version.major >= 26:
                uri = f"http://{instance.http_listen}/api/v1"

                liveness_request = requests.get(f"{uri}/health/live")
                assert liveness_request.status_code == 200, "wrong HTTP liveness probe response code"
                liveness_content = json.loads(liveness_request.content)
                assert liveness_content["status"] == "ok", "wrong HTTP liveness probe response answer"

                readiness_request = requests.get(f"{uri}/health/ready")
                assert readiness_request.status_code == 200, "wrong HTTP readiness probe response code"
                readiness_content = json.loads(readiness_request.content)
                assert readiness_content["status"] == "ok", "wrong HTTP readiness probe response answer"
            else:
                (current_state, _), _ = Retriable().call(Instance.states, instance)
                if current_state != "Online":
                    error = "invalid instance state"
                    hint = f"expected 'Online', got '{current_state}'"
                    raise AssertionError(f"{error}: {hint}")

    def _wait_cluster_buckets_are_balanced(self) -> None:
        self.cluster.wait_until_buckets_balanced(exclude=self.exclude)

    def _wait_governor_finish_activities(self) -> None:
        leader = Retriable().call(self.cluster.leader)
        leader.wait_governor_status("idle")

        queue = leader.sql("""
            SELECT op, op_format, status, status_description
            FROM _pico_governor_queue
            WHERE status != 'done';
        """)
        if len(queue) != 0:
            error = "governor was unable to finish his steps for too long"
            hint = f"the remaining unfinished steps are: {queue}"
            raise AssertionError(f"{error}, {hint}")

    def _verify_common_distributed_operations(self) -> None:
        message = "distributed DML operations check requires at least 3 alive instances"
        assert len(self.cluster.instances) >= 3, message

        instance = self.cluster.pick_random_instance(self.exclude)
        for engine in ["memtx", "vinyl"]:
            products_table = f"health_check_dist_products_{engine}"
            restock_table = f"health_check_dist_restock_{engine}"
            archive_table = f"health_check_dist_archive_{engine}"
            temporary_table = f"health_check_dist_temporary_{engine}"

            ##########################################################
            # Create multiple sharded tables.
            ##########################################################

            result = instance.sql(f"""
                CREATE TABLE {products_table} (
                    id INTEGER PRIMARY KEY,
                    name STRING,
                    stock_level INTEGER
                ) USING {engine}
                DISTRIBUTED BY (id)
                WAIT APPLIED GLOBALLY;
            """)
            assert result["row_count"] == 1

            result = instance.sql(f"""
                CREATE TABLE {restock_table} (
                    product_id INTEGER PRIMARY KEY,
                    add_quantity INTEGER
                ) USING {engine}
                DISTRIBUTED BY (product_id)
                WAIT APPLIED GLOBALLY;
            """)
            assert result["row_count"] == 1

            result = instance.sql(f"""
                CREATE TABLE {archive_table} (
                    id INTEGER PRIMARY KEY,
                    name STRING
                ) USING {engine}
                DISTRIBUTED BY (id)
                WAIT APPLIED GLOBALLY;
            """)
            assert result["row_count"] == 1

            result = instance.sql(f"""
                CREATE TABLE {temporary_table} (
                    id INTEGER PRIMARY KEY,
                    data STRING
                ) USING {engine}
                DISTRIBUTED BY (id)
                WAIT APPLIED GLOBALLY;
            """)
            assert result["row_count"] == 1

            ##########################################################
            # Extend table with secondary index.
            ##########################################################

            result = instance.sql(f"""
                CREATE INDEX idx_products_name
                ON {products_table} (name)
                WAIT APPLIED GLOBALLY;
            """)
            assert result["row_count"] == 1

            ##########################################################
            # Populate distributed tables.
            ##########################################################

            result = instance.sql(f"""
                INSERT INTO {products_table}
                VALUES
                    (1, 'Gadget', 100),
                    (2, 'Widget', 50),
                    (3, 'Doodad', 0);
            """)
            assert result["row_count"] == 3

            result = instance.sql(f"""
                INSERT INTO {restock_table}
                VALUES
                    (1, 25),
                    (2, 75);
            """)
            assert result["row_count"] == 2

            ##########################################################
            # Perform complex business-like logic.
            ##########################################################

            result = instance.sql(f"""
                SELECT name
                FROM {products_table}
                WHERE id = 2;
            """)
            assert result[0][0] == "Widget"

            result = instance.sql(f"""
                SELECT id
                FROM {products_table}
                WHERE name = 'Gadget';
            """)
            assert result[0][0] == 1

            result = instance.sql(f"""
                UPDATE {products_table}
                SET stock_level = {products_table}.stock_level + {restock_table}.add_quantity
                FROM {restock_table}
                WHERE {products_table}.id = {restock_table}.product_id;
            """)
            assert result["row_count"] == 2

            result = instance.sql(f"""
                INSERT INTO {archive_table} (id, name)
                    SELECT id, name
                    FROM {products_table}
                    WHERE stock_level = 0;
            """)
            assert result["row_count"] == 1

            result = instance.sql(f"""
                SELECT COUNT(*)
                FROM {archive_table};
            """)
            assert result[0][0] == 1

            result = instance.sql(f"""
                DELETE FROM {products_table}
                WHERE id IN (
                    SELECT id
                    FROM {archive_table}
                );
            """)
            assert result["row_count"] == 1

            result = instance.sql(f"""
                SELECT COUNT(*)
                FROM {products_table};
            """)
            assert result[0][0] == 2

            ##########################################################
            # A little bit of truncations.
            ##########################################################

            result = instance.sql(f"""
                INSERT INTO {temporary_table}
                VALUES
                    (1, 'test data 1'),
                    (2, 'test data 2');
            """)
            assert result["row_count"] == 2

            result = instance.sql(f"""
                SELECT COUNT(*)
                FROM {temporary_table};
            """)
            assert result[0][0] == 2

            result = instance.sql(f"""
                TRUNCATE TABLE {temporary_table}
                WAIT APPLIED GLOBALLY;
            """)
            assert result["row_count"] == 1

            result = instance.sql(f"""
                SELECT COUNT(*)
                FROM {temporary_table};
            """)
            assert result[0][0] == 0

            ##########################################################
            # Do not forget to clean up.
            ##########################################################

            for table in [products_table, restock_table, archive_table, temporary_table]:
                result = instance.sql(f"DROP TABLE IF EXISTS {table};")
                assert result["row_count"] == 1


def handle_process_dead(
    exception: ProcessDead,
    entity: HealthCheck,
) -> None:
    if entity.error is None:
        message = str(exception)
        version = entity.cluster.version()
        error = f"process died unexpectedly at version {version}"
        raise AssertionError(f"{error}: {message}") from exception

    if not entity.error.matches_exception(exception):
        error = "process died as expected, but the reason was different"
        message = f"expected '{entity.error.description}', got '{exception}'"
        raise AssertionError(f"{error}: {message}") from exception

    # The exception matched the expected error — nothing to propagate.
    return


def handle_tarantool_error(
    exception: TarantoolError,
    entity: HealthCheck,
) -> None:
    message = str(exception)
    # During rolling upgrades, DDL operations are expected to fail because
    # the cluster is running mixed versions. Suppress this specific error
    # so the rest of the health check can still verify SQL queries.
    if not entity.homogeneous and "DDL in heterogeneous cluster is prohibited" not in message:
        error = "unexpected Tarantool error during health check"
        raise AssertionError(f"{error}: {message}") from exception


def handle_any_exception(
    exception: Exception,
    entity: HealthCheck,
) -> None:
    if entity.error is None:
        raise exception

    # If we expect a log pattern, ensure crawlers were actually created.
    if entity.error.expects_log:
        error = f"no log crawlers for {entity.cluster} were found"
        message = f"but the following error in logs was expected: {entity.error.log_pattern}"
        assert entity.crawlers, f"{error}, {message}"

    # For exception-based expectations, check whether the raised exception matches.
    if not entity.error.expects_log:
        if not entity.error.matches_exception(exception):
            error = "health check error did not match expectation"
            message = f"expected {entity.error.description}, got '{exception}'"
            raise AssertionError(f"{error}: {message}") from exception
        return

    # For log-based expectations, verify the pattern appeared in logs.
    error_logged = False
    for crawler in entity.crawlers.values():
        if crawler is not None:
            try:
                crawler.wait_matched()
                error_logged = True
            except AssertionError:
                pass

    if not error_logged:
        pattern = entity.error.log_pattern
        error = f"the log pattern '{pattern}' was never matched"
        message = "neither as an exception, nor from logs"
        raise AssertionError(f"{error}: {message}")
