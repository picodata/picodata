import math
import time
import random
from conftest import Cluster, Instance

STEP_DELAY = 500  # ms


def create(c: Cluster, istate):
    i = c.add_instance(wait_online=False)
    istate[i.instance_id] = {"instance": i, "started": False}
    return i, istate


def stop(_: Cluster, i: Instance, istate):
    istate[i.instance_id]["started"] = False
    return i.terminate(), istate


def start(_: Cluster, i: Instance, istate):
    istate[i.instance_id]["started"] = True
    return i.start(), istate


def expel(c: Cluster, i: Instance, istate):
    istate[i.instance_id]["started"] = False
    return c.expel(i)


ADD = "add"
STOP = "stop"
START = "start"
EXPEL = "expel"
ACTIONS = {
    ADD: {
        "name": ADD,
        "repr_fn": lambda i: f"Add new instance {i}",
        "pre_fn": create,
        "exec_fn": start,
    },
    STOP: {
        "name": STOP,
        "repr_fn": lambda i: f"Stop {i}",
        "exec_fn": stop,
    },
    START: {
        "name": START,
        "repr_fn": lambda i: f"Start {i}",
        "exec_fn": start,
    },
    EXPEL: {
        "name": EXPEL,
        "repr_fn": lambda i: f"Expel {i}",
        "exec_fn": expel,
    },
}
BASE = len(ACTIONS)


def possible_actions(c: Cluster, istate):
    actions = []
    actions.append([ADD, None])
    stopping_allowed = stop_allowed(istate)
    for i in c.instances:
        if istate[i.instance_id]["started"] and i.process is not None:
            if stopping_allowed:
                actions.append([STOP, i])  # type: ignore
        elif not istate[i.instance_id]["started"] and i.process is None:
            actions.append([START, i])  # type: ignore
    return actions


def stop_allowed(istate):
    started_cnt = len(list(filter(lambda i: i[1]["started"], istate.items())))
    ndiv2_plus1 = math.trunc(len(istate) / 2 + 1)
    return started_cnt > ndiv2_plus1


def choose_action(c: Cluster, istate):
    actions = possible_actions(c, istate)
    a = actions[random.randint(0, len(actions) - 1)]
    return ACTIONS[a[0]], a[1]


def step_msg(step: int, action, i: Instance):
    msg = action["repr_fn"](i.instance_id if i and i.instance_id else None)
    return f"Step {step}: {msg}"


def initial_istate(cluster: Cluster):
    istate = {}
    for i in cluster.instances:
        istate[i.instance_id] = {"instance": i, "started": True}
    return istate


def test_randomized(cluster: Cluster, seed: str, delay: int, capsys):
    cluster.deploy(instance_count=3)

    random.seed(seed)

    delay = int(delay) if delay else STEP_DELAY

    with capsys.disabled():
        print(f"Seed: {seed} , step delay: {delay} ms")

    steps_count = random.randint(5, 10)
    print(f"Do {steps_count} steps...")

    istate = initial_istate(cluster)

    for step in range(steps_count):
        a, i = choose_action(cluster, istate)
        if "pre_fn" in a.keys():
            i, istate = a["pre_fn"](cluster, istate)
        print(step_msg(step + 1, a, i))
        _, istate = a["exec_fn"](cluster, i, istate)
        time.sleep(delay / 1000)

    for instance_id in istate:
        ist = istate[instance_id]
        if ist["started"]:
            ist["instance"].wait_online()
