#!/usr/bin/python3

import random

from py_eosio.chain.time import Microseconds


def us_to_sec(us: int) -> int:
    return int(us / 1000000)


def sec_to_us(s: float) -> int:
    return int(s * 1000000)


def test_us_init():
    num = random.randint(0, 100000)

    us = Microseconds(num)

    assert us.count() == num
    assert us.seconds() == us_to_sec(num)


def test_us_addition():
    num_a = random.randint(0, 100000)
    num_b = random.randint(0, 100000)

    a = Microseconds(num_a)
    b = Microseconds(num_b)

    assert (a + b).count() == num_a + num_b


def test_us_substraction():
    num_a = random.randint(0, 100000)

    # make sure b is less than a
    while (num_b := random.randint(0, 100000)) >= num_a:
        pass

    a = Microseconds(num_a)
    b = Microseconds(num_b)

    assert (a - b).count() == num_a - num_b


def test_us__gt__():
    num = random.randint(0, 100000)

    a = Microseconds(num * 10)
    b = Microseconds(num)

    assert a > b


def test_us__gteq__():
    num = random.randint(0, 100000)

    a = Microseconds(num)
    b = Microseconds(num * 10)

    assert b >= a

    c = Microseconds(num * 10)

    assert b >= c


def test_us__lt__():
    num = random.randint(0, 100000)

    a = Microseconds(num)
    b = Microseconds(num * 10)

    assert a < b


def test_us__lteq__():
    num = random.randint(0, 100000)

    a = Microseconds(num)
    b = Microseconds(num * 10)

    assert a <= b

    c = Microseconds(num)

    assert a <= c


def test_us_addition_sugar():
    num_a = random.randint(0, 100000)
    num_b = random.randint(0, 100000)

    a = Microseconds(num_a)
    b = Microseconds(num_b)

    a += b

    assert a.count() == num_a + num_b


def test_us_substraction_sugar():
    num_a = random.randint(0, 100000)

    # make sure b is less than a
    while (num_b := random.randint(0, 100000)) >= num_a:
        pass

    a = Microseconds(num_a)
    b = Microseconds(num_b)

    a -= b

    assert a.count() == num_a - num_b
