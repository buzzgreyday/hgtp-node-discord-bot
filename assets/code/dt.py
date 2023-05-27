import time
from datetime import datetime


def timing():
    return datetime.utcnow(), time.perf_counter()
