
import random

import hashlib
from hashlib import sha256, sha512

from py_eosio.chain.crypto import SHA256, SHA512, RIPEMD160


def memcmp(a, b):
    for ca, cb in zip(a, b):
        if ca < cb:
            return -1
        elif cb < ca:
            return 1
    return 0


def random_hexdigest(size: int = 64):
    return f'%0{size}x' % random.randrange(16 ** size)


def test_sha256_init():
    rand_hex = random_hexdigest()

    h = SHA256(rand_hex)

    assert str(h) == rand_hex


def test_sha256_hash_string():
    assert str(SHA256.hash_string('test')) == sha256(b'test').hexdigest()


def test_sha256__eq__():
    d = random_hexdigest()
    a = SHA256(d)
    b = SHA256(d)

    assert a == b


def test_sha256__neq__():
    assert SHA256(random_hexdigest()) != SHA256(random_hexdigest())


def test_sha256__gteq__():
    smaller = random_hexdigest()

    while memcmp((bigger := random_hexdigest()), smaller) < 0:
        pass

    assert SHA256(bigger) >= SHA256(smaller)


def test_sha256__gt__():
    smaller = random_hexdigest()

    while memcmp((bigger := random_hexdigest()), smaller) <= 0:
        pass

    assert SHA256(bigger) > SHA256(smaller)


def test_sha256__lt__():
    smaller = random_hexdigest()

    while memcmp((bigger := random_hexdigest()), smaller) <= 0:
        pass

    assert SHA256(smaller) < SHA256(bigger)


def test_sha512_init():
    rand_hex = random_hexdigest(size=128)

    h = SHA512(rand_hex)

    assert str(h) == rand_hex


def test_sha512_hash_string():
    assert str(SHA512.hash_string('test')) == sha512(b'test').hexdigest()


def test_sha512__eq__():
    d = random_hexdigest(size=128)
    a = SHA512(d)
    b = SHA512(d)

    assert a == b


def test_sha512__neq__():
    assert SHA512(random_hexdigest(size=128)) != SHA512(random_hexdigest(size=128))


def test_sha512__gteq__():
    smaller = random_hexdigest(size=128)

    while memcmp((bigger := random_hexdigest(size=128)), smaller) < 0:
        pass

    assert SHA512(bigger) >= SHA512(smaller)


def test_sha512__gt__():
    smaller = random_hexdigest(size=128)

    while memcmp((bigger := random_hexdigest(size=128)), smaller) <= 0:
        pass

    assert SHA512(bigger) > SHA512(smaller)


def test_sha512__lt__():
    smaller = random_hexdigest(size=128)

    while memcmp((bigger := random_hexdigest(size=128)), smaller) <= 0:
        pass

    assert SHA512(smaller) < SHA512(bigger)


def ripemd160(s: str):
    h = hashlib.new('ripemd160')
    h.update(s.encode('utf-8'))
    return h.hexdigest()


def test_ripemd160_init():
    rand_hex = random_hexdigest(size=40)

    h = RIPEMD160(rand_hex)

    assert str(h) == rand_hex


def test_ripemd160_hash_string():
    assert str(RIPEMD160.hash_string('test')) == ripemd160('test')


def test_ripemd160__eq__():
    d = random_hexdigest(size=40)
    a = RIPEMD160(d)
    b = RIPEMD160(d)

    assert a == b


def test_ripemd160__neq__():
    assert RIPEMD160(random_hexdigest(size=40)) != RIPEMD160(random_hexdigest(size=40))


def test_ripemd160__gteq__():
    smaller = random_hexdigest(size=40)

    while memcmp((bigger := random_hexdigest(size=40)), smaller) < 0:
        pass

    assert RIPEMD160(bigger) >= RIPEMD160(smaller)


def test_ripemd160__gt__():
    smaller = random_hexdigest(size=40)

    while memcmp((bigger := random_hexdigest(size=40)), smaller) <= 0:
        pass

    assert RIPEMD160(bigger) > RIPEMD160(smaller)


def test_ripemd160__lt__():
    smaller = random_hexdigest(size=40)

    while memcmp((bigger := random_hexdigest(size=40)), smaller) <= 0:
        pass

    assert RIPEMD160(smaller) < RIPEMD160(bigger)
