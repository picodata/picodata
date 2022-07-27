import time

SEED_CAP = 14  # 19 max


def int_to_base_62(num: int):
    base_62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    BASE = len(base_62)
    rete = ""
    while num != 0:
        rete = base_62[num % BASE] + rete
        num = num // BASE
    return rete


def generate_seed():
    return int_to_base_62(time.time_ns() % pow(10, SEED_CAP))
