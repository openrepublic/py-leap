#!/usr/bin/env python3

from __future__ import annotations

import re
import time
import json
import socket
import string
import random
import logging
import tarfile
import tempfile

from typing import Dict, Optional
from decimal import Decimal
from pathlib import Path
from hashlib import sha1
from datetime import datetime
from binascii import hexlify

from natsort import natsorted
from docker.errors import NotFound
from docker.models.containers import Container

from .typing import ExecutionStream, ExecutionResult


LEAP_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'


def string_to_sym_code(sym):
    ret = 0
    for i, char in enumerate(sym):
        if char >= 'A' or char <= 'Z':
            code = ord(char)
            ret |= code << 8 * i

    return ret


class Symbol:

    def __init__(self, code: str, precision: int):
        self.code = code
        self.precision = precision

    @property
    def unit(self) -> float:
        return 1 / (10 ** self.precision)

    def __eq__(self, other) -> bool:
        return (
            self.code == other.code and
            self.precision == other.precision
        )

    def __str__(self) -> str:
        return f'{self.precision},{self.code}'


def symbol_from_str(str_sym: str):
    prec, code = str_sym.split(',')
    return Symbol(code, int(prec))


class Asset:

    def __init__(self, amount: float, symbol: Symbol):
        self.amount = amount
        self.symbol = symbol

    def __eq__(self, other) -> bool:
        return (
            self.amount == other.amount and
            self.symbol == other.symbol
        )

    def __str__(self) -> str:
        number = format(self.amount, f'.{self.symbol.precision}f')
        return f'{number} {self.symbol.code}'


def asset_from_str(str_asset: str):
    numeric, sym = str_asset.split(' ')
    if '.' not in numeric:
        precision = 0
    else:
        precision = len(numeric) - numeric.index('.') - 1
    return Asset(float(numeric), Symbol(sym, precision))


def asset_from_decimal(dec: Decimal, precision: int, sym: str):
    result = str(dec)
    pindex = result.index('.')
    return f'{result[:pindex + 1 + precision]} {sym}'


def asset_from_ints(amount: int, precision: int, sym: str):
    result = str(amount)
    return f'{result[:-precision]}.{result[-precision:]} {sym}'


def leap_format_date(date: datetime) -> str:
    return date.strftime(LEAP_DATE_FORMAT)


def leap_parse_date(date: str) -> datetime:
    return datetime.strptime(date, LEAP_DATE_FORMAT)


class Name:

    def __init__(self, _str: str):

        assert len(_str) <= 13
        assert not bool(re.compile(r'[^a-z0-9.]').search(_str))

        self._str = _str

    def __str__(self) -> str:
        return self._str

    @property
    def value(self) -> int:
        """Convert name to its number repr
        """

        def str_to_hex(c):
            hex_data = hexlify(bytearray(c, 'ascii')).decode()
            return int(hex_data, 16)


        def char_subtraction(a, b, add):
            x = str_to_hex(a)
            y = str_to_hex(b)
            ans = str((x - y) + add)
            if len(ans) % 2 == 1:
                ans = '0' + ans
            return int(ans)


        def char_to_symbol(c):
            ''' '''
            if c >= 'a' and c <= 'z':
                return char_subtraction(c, 'a', 6)
            if c >= '1' and c <= '5':
                return char_subtraction(c, '1', 1)
            return 0

        i = 0
        name = 0
        while i < len(self._str) :
            name += (char_to_symbol(self._str[i]) & 0x1F) << (64-5 * (i + 1))
            i += 1
        if i > 12 :
            name |= char_to_symbol(self._str[11]) & 0x0F
        return name


def name_from_value(n: int) -> Name:
    """Convert valid leap name value to the internal representation
    """
    charmap = '.12345abcdefghijklmnopqrstuvwxyz'
    name = ['.'] * 13
    i = 0
    while i <= 12:
        c = charmap[n & (0x0F if i == 0 else 0x1F)]
        name[12-i] = c
        n >>= 4 if i == 0 else 5
        i += 1
    return Name(''.join(name).rstrip('.'))


def str_to_hex(c):
    hex_data = hexlify(bytearray(c, 'ascii')).decode()
    return int(hex_data, 16)


def char_subtraction(a, b, add):
    x = str_to_hex(a)
    y = str_to_hex(b)
    ans = str((x - y) + add)
    if len(ans) % 2 == 1:
        ans = '0' + ans
    return int(ans)


def char_to_symbol(c):
    ''' '''
    if c >= 'a' and c <= 'z':
        return char_subtraction(c, 'a', 6)
    if c >= '1' and c <= '5':
        return char_subtraction(c, '1', 1)
    return 0


def string_to_name(s: str) -> int:
    """Convert valid leap name to its number repr
    """
    i = 0
    name = 0
    while i < len(s) :
        name += (char_to_symbol(s[i]) & 0x1F) << (64-5 * (i + 1))
        i += 1
    if i > 12 :
        name |= char_to_symbol(s[11]) & 0x0F
    return name


def name_to_string(n: int) -> str:
    """Convert valid leap name to its ascii repr
    """
    charmap = '.12345abcdefghijklmnopqrstuvwxyz'
    name = ['.'] * 13
    i = 0
    while i <= 12:
        c = charmap[n & (0x0F if i == 0 else 0x1F)]
        name[12-i] = c
        n >>= 4 if i == 0 else 5
        i += 1
    return ''.join(name).rstrip('.')


def find_in_balances(balances, symbol):
    for balance in balances:
        asset = asset_from_str(balance['balance'])
        if asset.symbol == symbol:
            return asset

    return None


def collect_stdout(out: Dict):
    assert isinstance(out, dict)
    output = ''
    for action_trace in out['processed']['action_traces']:
        if 'console' in action_trace:
            output += action_trace['console']

    return output


class Checksum256:

    def __init__(self, h: str):
        self._hash = h

    def __str__(self) -> str:
        return self._hash


# SHA-1 hash of file
def hash_file(path: Path) -> bytes:
    BUF_SIZE = 65536
    hasher = sha1()
    with open(path, 'rb') as target_file:
        while True:
            data = target_file.read(BUF_SIZE)
            if not data:
                break
            hasher.update(data)

    return hasher.digest()


def hash_dir(target: Path, includes=[]):
    logging.info(f'hashing: {target}')
    hashes = []
    files_done = set()
    files_todo = {
        *[node.resolve() for node in target.glob('**/*.cpp')],
        *[node.resolve() for node in target.glob('**/*.hpp')],
        *[node.resolve() for node in target.glob('**/*.c')],
        *[node.resolve() for node in target.glob('**/*.h')]
    }
    while len(files_todo) > 0:
        new_todo = set()
        for node in files_todo:

            if node in files_done:
                continue

            if not node.is_file():
                files_done.add(node)
                continue

            hashes.append(hash_file(node))
            files_done.add(node)
            with open(node, 'r') as source_file:
                src_contents = source_file.read()

            # Find all includes in source & add to todo list
            for match in re.findall('(#include )(.+)\n', src_contents):
                assert len(match) == 2
                match = match[1]
                include = match.split('<')
                if len(include) == 1:
                    include = match.split('\"')[1]
                else:
                    include = include[1].split('>')[0]

                for include_path in includes:
                    new_path = Path(f'{include_path}/{include}').resolve()
                    if new_path in files_done:
                        continue
                    new_todo.add(new_path)

                logging.info(f'found include: {include}')

        files_todo = new_todo

    # Order hashes and compute final hash
    hasher = sha1()
    for file_digest in natsorted(hashes, key=lambda x: x.lower()):
        hasher.update(file_digest)

    _hash = hasher.hexdigest()
    return _hash

#
# data generators for testing
#

def random_string(size=256):
    return ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in range(size)
    )

def random_local_url():
    return f'http://localhost/{random_string(size=16)}'


def random_token_symbol():
    return ''.join(
        random.choice(string.ascii_uppercase)
        for _ in range(3)
    )

def random_leap_name():
    return ''.join(
        random.choice('12345abcdefghijklmnopqrstuvwxyz')
        for _ in range(12)
    )


# docker helpers

def wait_for_attr(
    cntr: Container,
    attr_path: Tuple[str],
    expect=None,
    timeout=20
):
    """Wait for a container's attr value to be set.
    If ``expect`` is provided wait for the value to be set to that value.
    """
    def get(val, path):
        for key in path:
            try:
                val = val[key]

            except KeyError as ke:
                raise KeyError(f'{key} not found in {val}')

        return val

    start = time.time()
    while time.time() - start < timeout:
        cntr.reload()
        val = get(cntr.attrs, attr_path)
        if expect is None and val:
            return val
        elif val == expect:
            return val
    else:
        raise TimeoutError("{} failed to be {}, value: \"{}\"".format(
            attr_path, expect if expect else 'not None', val))

def get_container(
    dockerctl,
    image: str,
    force_unique: bool = False,
    logger=None,
    *args, **kwargs
):
    """
    Get already running container or start one up using an existing dockerctl
    instance.
    """
    if logger is None:
        logger = logging.getLogger()

    if not force_unique:
        found = dockerctl.containers.list(
            filters={
                'ancestor': image,
                'status': 'running'
            }
        )
        if len(found) > 0:

            if len(found) > 1:
                logger.warning('Found more than one posible cdt container')

            return found[0]

    local_images = []
    for img in dockerctl.images.list(all=True):
        local_images += img.tags

    if image not in local_images:
        splt_image = image.split(':')
        if len(splt_image) == 2:
            repo, tag = splt_image
        else:
            raise ValueError(
                f'Expected \'{image}\' to have \'repo:tag\' format.') 

        updates = {}
        for update in dockerctl.api.pull(
            repo, tag=tag, stream=True, decode=True
        ):
            if 'id' in update:
                _id = update['id']
                if _id not in updates or (updates[_id] != update['status']):
                    updates[_id] = update['status']
                    logger.info(f'{_id}: {update["status"]}')

    return dockerctl.containers.run(image, *args, **kwargs)


def docker_open_process(
    client,
    cntr,
    cmd: List[str],
    **kwargs
) -> ExecutionStream:
    """Begin running the command inside the container, return the
    internal docker process id, and a stream for the standard output.

    :param cmd: List of individual string forming the command to execute in
        the testnet container shell.
    :param kwargs: A variable number of key word arguments can be
        provided, as this function uses `exec_create & exec_start docker APIs
        <https://docker-py.readthedocs.io/en/stable/api.html#module-dock
        er.api.exec_api>`_.

    :return: A tuple with the process execution id and the output stream to
        be consumed.
    :rtype: :ref:`typing_exe_stream`
    """
    exec_id = client.api.exec_create(cntr.id, cmd, **kwargs)
    exec_stream = client.api.exec_start(exec_id=exec_id, stream=True)
    return exec_id['Id'], exec_stream

def docker_wait_process(
    client,
    exec_id: str,
    exec_stream: Iterator[str],
    logger=None
) -> ExecutionResult:
    """Collect output from process stream, then inspect process and return
    exitcode.

    :param exec_id: Process execution id provided by docker engine.
    :param exec_stream: Process output stream to be consumed.

    :return: Exitcode and process output.
    :rtype: :ref:`typing_exe_result`
    """
    if logger is None:
        logger = logging.getLogger()

    out = ''
    for chunk in exec_stream:
        msg = chunk.decode('utf-8')
        out += msg

    info = client.api.exec_inspect(exec_id)

    ec = info['ExitCode']
    if ec != 0:
        logger.warning(out.rstrip())

    return ec, out


def docker_move_into(
    client,
    container: Union[str, Container],
    src: Union[str, Path],
    dst: Union[str, Path]
):
    tmp_name = random_string(size=32)
    archive_loc = Path(f'/tmp/{tmp_name}.tar.gz').resolve()

    with tarfile.open(archive_loc, mode='w:gz') as archive:
        archive.add(src, recursive=True)

    with open(archive_loc, 'rb') as archive:
        binary_data = archive.read()

    archive_loc.unlink()

    if isinstance(container, Container):
        container = container.id

    client.api.put_archive(container, dst, binary_data)


def docker_move_out(
    client,
    container: Union[str, Container],
    src: Union[str, Path],
    dst: Union[str, Path]
):
    tmp_name = random_string(size=32)
    archive_loc = Path(f'/tmp/{tmp_name}.tar.gz').resolve()

    bits, stats = container.get_archive(src, encode_stream=True)

    with open(archive_loc, mode='wb+') as archive:
        for chunk in bits:
            archive.write(chunk)

    extract_path = Path(dst).resolve()

    if extract_path.is_file():
        extract_path = extract_path.parent

    with tarfile.open(archive_loc, 'r') as archive:
        archive.extractall(path=extract_path)

    archive_loc.unlink()


def get_free_port(tries=10):
    _min = 10000
    _max = 60000
    found = False

    for i in range(tries):
        port_num = random.randint(_min, _max)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            s.bind(("127.0.0.1", port_num))
            s.close()

        except socket.error as e:
            continue

        else:
            return port_num


import requests
from urllib.request import urlretrieve
import zstandard as zstd

def download_latest_snapshot(
    target_path: Path, network='telos', version='v6'
):
    _repository_url = 'https://snapshots.eosnation.io'

    # first open repo to get filename
    url = _repository_url + f'/{network}-{version}/latest'
    file_info = requests.head(url, allow_redirects=True)
    filename = file_info.url.split('/')[-1]

    file_path = target_path / filename

    # finally retrieve
    urlretrieve(file_info.url, file_path)

    dec_file_path = target_path / file_path.stem
    dctx = zstd.ZstdDecompressor()
    with open(file_path, 'rb') as ifh:
        with open(dec_file_path, 'wb') as ofh:
            dctx.copy_stream(ifh, ofh)

    file_path.unlink()

    block_num = int((filename.split('-')[-1]).split('.')[0])

    return block_num, dec_file_path
