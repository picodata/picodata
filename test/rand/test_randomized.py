import sys
import time
import random
import os
import signal
from conftest import Cluster, Instance, ReturnError
import pathlib


def int_to_base_62(num: int):
    base_62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    BASE = len(base_62)
    rete = ""
    while num != 0:
        rete = base_62[num % BASE] + rete
        num = num // BASE
    return rete


def create(c: Cluster, istate):
    i = c.add_instance(wait_ready=False)
    istate[i.instance_id] = {"instance": i, "started": False}
    return i, istate


def stop(i: Instance, istate):
    assert i.process, f"Instance {i.instance_id} expected has process"
    assert i.process.pid, f"Instance {i.instance_id} expected process to be alive"
    istate[i.instance_id]["started"] = False
    return os.kill(i.process.pid, signal.SIGTERM), istate


def start(i: Instance, istate):
    istate[i.instance_id]["started"] = True
    return i.start(), istate


ADD = "add"
STOP = "stop"
START = "start"
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
}
BASE = len(ACTIONS)

SEEDLOG = "seeds.txt"


def possible_actions(c: Cluster, istate):
    actions = []
    actions.append([ADD, None])
    for i in c.instances:
        if istate[i.instance_id]["started"]:
            actions.append([STOP, i])
        else:
            actions.append([START, i])
    return actions


def choose_action(c: Cluster, istate):
    actions = possible_actions(c, istate)
    a = actions[random.randint(0, len(actions) - 1)]
    return ACTIONS[a[0]], a[1]


def step_msg(step: int, action, i: Instance):
    msg = action["repr_fn"](i.instance_id if i and i.instance_id else None)
    return f"Step {step}: {msg}"


def get_seed():
    return int_to_base_62(time.time_ns())


def log_seed(seed):
    dir = pathlib.Path(__file__).parent.absolute()
    with open(os.path.join(dir, SEEDLOG), "a") as f:
        f.write(seed + "\n")


def test_randomized(cluster: Cluster, seed: int, capsys):
    cluster.deploy(instance_count=3)

    seed = seed if seed else get_seed()
    log_seed(seed)
    with capsys.disabled():
        print(f"Seed: {seed}")

    random.seed(seed)

    steps_count = random.randint(5, 10)
    print(f"Do {steps_count} steps...")

    istate = {}
    for i in cluster.instances:
        istate[i.instance_id] = {"instance": i, "started": True}

    for step in range(steps_count):
        a, i = choose_action(cluster, istate)
        if "pre_fn" in a.keys():
            i, istate = a["pre_fn"](cluster, istate)
        print(step_msg(step + 1, a, i))
        _, istate = a["exec_fn"](i, istate)
        time.sleep(0.1)

    for instance_id in istate:
        ist = istate[instance_id]
        if ist["started"]:
            ist["instance"].wait_ready()
