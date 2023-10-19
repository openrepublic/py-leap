#!/usr/bin/env python3

from __future__ import annotations

import re
import time
import socket
import string
import random
import logging
import tarfile

from typing import Any, Dict, List, Optional
from decimal import Decimal
from pathlib import Path
from hashlib import sha1
from datetime import datetime
from binascii import hexlify

from natsort import natsorted


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

    def __init__(self, amount: int, symbol: Symbol):
        self.amount = amount
        self.symbol = symbol

    def __eq__(self, other) -> bool:
        return (
            self.amount == other.amount and
            self.symbol == other.symbol
        )

    def __str__(self) -> str:
        str_amount = str(self.amount).zfill(self.symbol.precision + 1)
        if self.symbol.precision:
            return f'{str_amount[:-self.symbol.precision]}.{str_amount[-self.symbol.precision:]} {self.symbol.code}'
        return f'{str_amount} {self.symbol.code}'

    def to_decimal(self) -> Decimal:
        str_amount = str(self.amount).zfill(self.symbol.precision + 1)
        if self.symbol.precision:
            str_amount = f'{str_amount[:-self.symbol.precision]}.{str_amount[-self.symbol.precision:]}'

        return Decimal(str_amount)


def asset_from_str(str_asset: str):
    numeric, sym = str_asset.split(' ')

    if '.' not in numeric:
        precision = 0

    else:
        precision = len(numeric) - numeric.index('.') - 1
        numeric = numeric.replace('.', '')

    return Asset(int(numeric), Symbol(sym, precision))


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


from msgspec import Struct


class LeapOptional(Struct):
    value: Any
    type: str


class UInt8(Struct):
    num: int


class UInt16(Struct):
    num: int


class UInt32(Struct):
    num: int


class UInt64(Struct):
    num: int


class VarUInt32(Struct):
    num: int


class Int8(Struct):
    num: int


class Int16(Struct):
    num: int


class Int32(Struct):
    num: int


class Int64(Struct):
    num: int


class VarInt32(Struct):
    num: int


class Checksum160(Struct):
    hash: str

    def __str__(self) -> str:
        return self.hash


class Checksum256(Struct):
    hash: str

    def __str__(self) -> str:
        return self.hash


class ListArgument(Struct):
    list: List
    type: str


class PermissionLevel(Struct):
    actor: str
    permission: str

    def get_dict(self) -> dict:
        return {
            'actor': self.actor,
            'permission': self.permission
        }


class PermissionLevelWeight(Struct):
    permission: PermissionLevel
    weight: int

    def get_dict(self) -> dict:
        return {
            'permission': self.permission.get_dict(),
            'weight': self.weight
        }


class PublicKey(Struct):
    key: str

    def get(self) -> str:
        return self.key


class KeyWeight(Struct):
    key: PublicKey
    weight: int

    def get_dict(self) -> dict:
        return {
            'key': self.key.get(),
            'weight': self.weight
        }


class WaitWeight(Struct):
    wait: int
    weight: int

    def get_dict(self) -> dict:
        return {
            'wait_sec': self.wait,
            'weight': self.weight
        }


class Authority(Struct):
    threshold: int
    keys: List[KeyWeight]
    accounts: List[PermissionLevelWeight]
    waits: List[WaitWeight]

    def get_dict(self) -> dict:
        return {
            'threshold': self.threshold,
            'keys': [k.get_dict() for k in self.keys],
            'accounts': [a.get_dict() for a in self.accounts],
            'waits': [w.get_dict() for w in self.waits]
        }

class Abi(Struct):
    abi: dict

    def get_dict(self) -> dict:
        return self.abi


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



def download_latest_snapshot(
    target_path: Path, network='telos', version='v6'
):
    import requests
    from urllib.request import urlretrieve
    import zstandard as zstd

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


import os
import tarfile
import logging
from pathlib import Path
from typing import Optional

def download_with_progress_bar(url: str, file_path: Path) -> None:
    try:
        from tqdm import tqdm
        TQDM_AVAILABLE = True
    except ImportError:
        TQDM_AVAILABLE = False

    if not TQDM_AVAILABLE:
        raise ImportError('tqdm is not installed')

    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024

    with open(file_path, 'wb') as file:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=file_path.name) as bar:
            for data in response.iter_content(block_size):
                bar.update(len(data))
                file.write(data)

def download_snapshot(
    target_path: Path, block_number: int,
    network: str = 'mainnet', progress: bool = False,
    force_download: bool = False
) -> Optional[Path]:
    """Download the closest snapshot for a given block number.

    Args:
        target_path (Path): The path where the snapshot will be saved.
        block_number (int): The target block number.
        network (str, optional): The network ('mainnet' or 'testnet'). Defaults to 'mainnet'.
        progress (bool, optional): Whether to show a progress bar. Defaults to False.
        force_download (bool, optional): Whether to force download even if the file exists. Defaults to False.

    Returns:
        Optional[Path]: The path to the downloaded snapshot, or None if not found.
    """
    from bs4 import BeautifulSoup
    from urllib.request import urlretrieve
    import requests

    if network == 'mainnet':
        _repository_url = 'https://snapshots.telosunlimited.io/'
    else:
        _repository_url = 'https://snapshots.testnet.telosunlimited.io/'

    logging.info('Fetching snapshot list...')
    response = requests.get(_repository_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Get all snapshots and sort them in descending order by block number
    snapshots = []
    for row in soup.find('table', {'id': 'list'}).find('tbody').find_all('tr'):
        file_name = row.find('a').text
        if file_name.endswith('.tar.gz') and 'blk-' in file_name:
            try:
                snapshot_block_num = int(file_name.split('blk-')[-1].split('.')[0])
                snapshots.append((snapshot_block_num, file_name))
            except ValueError:
                continue

    snapshots.sort(reverse=True, key=lambda x: x[0])

    # Find the snapshot with the highest block number that is <= specified block number
    closest_snapshot = None
    for snapshot_block_num, file_name in snapshots:
        if snapshot_block_num <= block_number:
            closest_snapshot = file_name
            break

    if closest_snapshot is None:
        raise ValueError('No suitable snapshot found.')

    # Determine the final .bin file name
    final_bin_file_name = closest_snapshot.split('.tar.gz')[0] + '.snapshot.bin'
    final_bin_file_path = target_path / final_bin_file_name

    # Check if the .bin file already exists
    if final_bin_file_path.exists() and not force_download:
        logging.info(f'Snapshot file {final_bin_file_name} already exists. Skipping download.')
        return final_bin_file_path

    # Download the file
    file_url = _repository_url + closest_snapshot
    file_path = target_path / closest_snapshot
    logging.info(f'Downloading snapshot: {file_url}')

    if progress:
        download_with_progress_bar(file_url, file_path)
    else:
        urlretrieve(file_url, file_path)

    # Decompress the tar.gz
    temp_extract_path = target_path / file_path.stem
    logging.info(f'Decompressing snapshot to: {temp_extract_path}')
    with tarfile.open(file_path, 'r:gz') as tar_ref:
        tar_ref.extractall(path=temp_extract_path)

    # Move and rename the .bin file
    temp_extract_path = target_path / file_path.stem
    bin_file_name = next(f for f in os.listdir(temp_extract_path) if f.endswith('.bin'))
    bin_file_path = temp_extract_path / bin_file_name
    os.rename(bin_file_path, final_bin_file_path)

    # Clean up temporary files and directories
    logging.info('Cleaning up temporary files...')
    file_path.unlink()
    os.rmdir(temp_extract_path)

    logging.info('Operation completed successfully.')
    return final_bin_file_path
