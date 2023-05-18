import time
from datetime import datetime


def start_timing():
    return datetime.utcnow(), time.perf_counter()
