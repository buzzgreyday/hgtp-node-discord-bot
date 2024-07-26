import time
from datetime import datetime


def timing():
    return datetime.now(), time.perf_counter()
