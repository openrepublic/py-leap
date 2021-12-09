
import random

from hashlib import sha256

from py_eosio.chain.crypto import SHA256


def memcmp(a, b):
    for ca, cb in zip(a, b):
        if ca < cb:
            return -1
        elif cb < ca:
            return 1
    return 0


def random_hexdigest():
    return '%064x' % random.randrange(16**64)


def test_sha256_init():
    rand_hex = random_hexdigest()

    h = SHA256(rand_hex)

    assert str(h) == rand_hex


def test_sha256_hash_string():
    assert str(SHA256.hash_str('test')) == sha256(b'test').hexdigest()


def test_sha256__eq__():
    d = random_hexdigest()
    a = SHA256(d)
    b = SHA256(d)

    assert a == b


def test_sha256__neq__():
    assert SHA256(random_hexdigest()) != SHA256(random_hexdigest())


def test_sha256__gteq__():
    smaller = random_hexdigest()

    while memcmp((bigger := random_hexdigest()), smaller) <= 0:
        pass

    assert SHA256(bigger) >= SHA256(smaller)
