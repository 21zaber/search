import time

MEMORY_BASE = 1000
MEMORY_NAMES = ['', 'K', 'M', 'G', 'T', 'E']

TIME_BASE = [60, 60, 24, 365]
TIME_NAMES = ['s', 'm', 'h', 'y']

def format_memory(n, base=MEMORY_BASE, sufix='b'):
    p = 0
    b = base
    while n > b:
        b *= base
        p += 1

    b //= base

    return '{:.3f} {}{}'.format(n/b, MEMORY_NAMES[p], sufix)

def format_time(n, sufix=''):
    p = 0
    while n > 2 * TIME_BASE[p]:
        n /= TIME_BASE[p]
        p += 1

    return '{:.4f} {}{}'.format(n, TIME_NAMES[p], sufix)

def get_ts():
    return time.strftime("%Y.%m.%d %H:%M:%S", time.gmtime())

def log(*args, **kwargs):
    s = '[{ts}]'.format(ts=get_ts())
    print(s, *args, **kwargs)
