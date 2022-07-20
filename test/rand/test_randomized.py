import time
import random
import os
import signal
from conftest import Cluster, Instance


def int_to_base_62(num: int):
    base_62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    BASE = len(base_62)
    rete = ""
    while num != 0:
        rete = base_62[num % BASE] + rete
        num = num // BASE
    return rete


def add(c: Cluster, _):
    return c.add_instance()


def stop(_, i: Instance):
    assert i.process, f"Instance {i.instance_id} expected has process"
    assert i.process.pid, f"Instance {i.instance_id} expected process to be alive"
    return os.kill(i.process.pid, signal.SIGTERM)


def start(_, i: Instance):
    return i.start()


ACTIONS = {
    "add": {
        "repr": "Add",
        "fn": add,
    },
    "stop": {
        "repr": "Stop",
        "fn": stop,
    },
    "start": {
        "repr": "Start",
        "fn": start,
    },
}
BASE = len(ACTIONS)


def possible_actions(i: Instance | None):
    if i is None:
        return ["add"]
    elif i.process is not None:
        return ["stop"]
    else:
        return ["start"]


def choose_instance(cluster: Cluster):
    if len(cluster.instances) < 2:
        return None
    new_instance_chance = random.randint(0, 3) % 3 == 0
    if new_instance_chance:
        return None
    else:
        return cluster.instances[random.randint(0, len(cluster.instances) - 1)]


def choose_action(i: Instance | None):
    actions = possible_actions(i)
    return ACTIONS[actions[random.randint(0, len(actions) - 1)]]


def step_msg(step: int, action, i: Instance):
    if i is None:
        msg = f"action {action['repr']}"
    else:
        msg = f"action {action['repr']} for {i.instance_id}"
    return f"Step {step}: {msg}"


def test_randomized(cluster: Cluster, seed: int):
    seed = seed if seed else int_to_base_62(time.time_ns())
    print(f"Seed: {seed}")
    random.seed(seed)

    steps_count = random.randint(1, 5)
    print(f"Do {steps_count} steps...")

    for step in range(steps_count):
        i = choose_instance(cluster)
        a = choose_action(i)
        print(step_msg(step, a, i))
        a["fn"](cluster, i)
