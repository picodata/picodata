from conftest import Cluster


def test_dql_truncate_concurrent(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1 = cluster.instances[0]
    i2 = cluster.instances[1]

    preemption = True
    interval_us = 0
    opcode_cnt = 1

    i1.sql(
        f"""
        alter system set sql_preemption = {preemption}
    """
    )
    i1.sql(
        f"""
        alter system set sql_preemption_interval_us = {interval_us}
    """
    )
    i1.sql(
        f"""
        alter system set sql_preemption_opcode_max = {opcode_cnt}
    """
    )
    i1.sql(
        """
        alter system set sql_motion_row_max = 0
    """
    )
    i1.sql(
        """
        alter system set sql_vdbe_opcode_max = 0
    """
    )

    ddl = i1.sql(
        """
        create table t (a int, primary key (a))
        using memtx
        distributed by (a)
        wait applied globally
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    assert i1.sql(
        """
        select key, value from _pico_db_config where key like '%sql_preemption%';
    """
    ) == [
        ["sql_preemption", preemption],
        ["sql_preemption_interval_us", interval_us],
        ["sql_preemption_opcode_max", opcode_cnt],
    ]

    import time

    time.sleep(1)

    values_to_insert = ""
    for i in range(1000):
        values_to_insert += f"({i}),"
    values_to_insert += "(1000)"

    import threading
    import sys

    N = 10

    start_barrier = threading.Barrier(3)
    done_barrier = threading.Barrier(3)

    exc_holder = {"exc": None}
    exc_lock = threading.Lock()

    def fail_now():
        try:
            start_barrier.abort()
        except threading.BrokenBarrierError:
            pass
        try:
            done_barrier.abort()
        except threading.BrokenBarrierError:
            pass

    def record_and_fail():
        with exc_lock:
            if exc_holder["exc"] is None:
                exc_holder["exc"] = sys.exc_info()
        fail_now()

    def run_dql():
        for _ in range(N):
            try:
                start_barrier.wait()
            except Exception:
                record_and_fail()

            try:
                i1.sql("""
                    select *
                    from t t1
                    join t t2 on t1.a = t2.a
                    where t1.a > 1
                    order by t1.a
                    limit 10
                """)
            except Exception as err:
                err_str = str(err)
                if "space cursor is no longer valid" not in err_str:
                    record_and_fail()

            try:
                done_barrier.wait()
            except Exception:
                pass

    def run_truncate():
        try:
            for _ in range(N):
                try:
                    start_barrier.wait()
                except Exception:
                    pass

                i2.sql("truncate t")

                try:
                    done_barrier.wait()
                except Exception:
                    pass

        except Exception:
            record_and_fail()

    t1 = threading.Thread(target=run_dql)
    t2 = threading.Thread(target=run_truncate)
    t1.start()
    t2.start()

    try:
        for _ in range(N):
            if exc_holder["exc"]:
                break

            i2.sql(f"insert into t values {values_to_insert}")

            try:
                start_barrier.wait()
            except Exception:
                pass

            try:
                done_barrier.wait()
            except Exception:
                pass

    except Exception:
        record_and_fail()

    finally:
        fail_now()
        t1.join()
        t2.join()

    if exc_holder["exc"]:
        _, ev, tb = exc_holder["exc"]
        raise ev.with_traceback(tb)
