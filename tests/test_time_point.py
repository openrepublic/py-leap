#!/usr/bin/python3

import time
import random

from py_eosio.chain.time import Microseconds, TimePoint


def test_timepoint_init():
    num = random.randint(0, 1000000)
    us = Microseconds(num)

    timep = TimePoint(us)

    assert timep.since_epoch().count() == us.count()


def test_timepoint_now():
    now = TimePoint.now()

    assert now.sec_since_epoch() == int(time.time())


def test_timepoint_max():
    timep = TimePoint.max()
    val = timep.since_epoch().count()
    max_int_64 = (2 ** 63) - 1

    assert val == max_int_64


def test_timepoint_min():
    timep = TimePoint.min()
    val = timep.since_epoch().count()

    assert val == 0


def test_timepoint__gt__():
    t_max = TimePoint.max()
    t_min = TimePoint.min()

    assert t_max > t_min


def test_timepoint__gteq__():
    t_max = TimePoint.max()
    t_min = TimePoint.min()
    t_same = TimePoint(t_max.since_epoch())

    assert t_max >= t_min
    assert t_max >= t_same


def test_timepoint__lt__():
    t_max = TimePoint.max()
    t_min = TimePoint.min()

    assert t_min < t_max


def test_timepoint__lteq__():
    t_max = TimePoint.max()
    t_min = TimePoint.min()
    t_same = TimePoint(t_min.since_epoch())

    assert t_min <= t_max
    assert t_min <= t_same


def test_timepoint__eq__():
    num = random.randint(0, 1000000)
    t_a = TimePoint(Microseconds(num))
    t_b = TimePoint(Microseconds(num))

    assert t_a == t_b


def test_timepoint__neq__():
    t_max = TimePoint.max()
    t_min = TimePoint.min()

    assert t_min != t_max


def test_timepoint_microseconds_addition_sugar():
    num_a = random.randint(0, 1000000)
    t = TimePoint(Microseconds(num_a))

    num_b = random.randint(0, 1000000)
    us = Microseconds(num_b)

    t += us
    
    assert t.since_epoch().count() == num_a + num_b


def test_timepoint_microseconds_substraction_sugar():
    num_a = random.randint(0, 1000000)
    t = TimePoint(Microseconds(num_a))

    while (num_b := random.randint(0, 1000000)) >= num_a:
        pass

    us = Microseconds(num_b)

    t -= us
    
    assert t.since_epoch().count() == num_a - num_b


def test_timepoint_microseconds_addition():
    num_a = random.randint(0, 1000000)
    t = TimePoint(Microseconds(num_a))

    num_b = random.randint(0, 1000000)
    us = Microseconds(num_b)

    assert (t + us).since_epoch().count() == num_a + num_b


def test_timepoint_microseconds_substraction():
    num_a = random.randint(0, 1000000)
    t = TimePoint(Microseconds(num_a))

    while (num_b := random.randint(0, 1000000)) >= num_a:
        pass
    us = Microseconds(num_b)

    assert (t - us).since_epoch().count() == num_a - num_b
