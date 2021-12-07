#!/usr/bin/python3

from py_eosio.chain import Name


def test_name_init():
    n = Name()

    assert str(n) == ''
    assert n.as_int() == 0


def test_name_init_string():
    n_str = 'helloworld'
    n = Name(n_str)

    assert str(n) == n_str


def test_name_init_int():
    num = 42
    n = Name(num)

    assert n.as_int() == num


def test_name__lt__():
    num = 42

    a = Name(num)
    b = Name(num + 1)

    assert a < b


def test_name__gt__():
    num = 42

    a = Name(num)
    b = Name(num + 1)

    assert b > a


def test_name__lteq__():
    num = 42

    a = Name(num)
    b = Name(num + 1)

    assert a <= b

    c = Name(num + 1)

    assert b <= c


def test_name__gteq__():
    num = 42

    a = Name(num)
    b = Name(num + 1)

    assert b >= a

    c = Name(num + 1)

    assert b >= c


def test_name__eq__():
    a = Name("equality")
    b = Name(a.as_int())

    assert a == b


def test_name__neq__():
    a = Name("different")
    b = Name(a.as_int() + 1)

    assert a != b


def test_name__eq__with_int():
    a = Name("test")
    num = a.as_int()

    assert a == num


def test_name__neq__with_int():
    a = Name("test")
    num = a.as_int()

    assert a != num + 1
