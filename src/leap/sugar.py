#!/usr/bin/env python3

from __future__ import annotations

import socket
import string
import random
import logging
import tarfile

from pathlib import Path


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

    for _ in range(tries):
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

    reported = []
    def _maybe_report_download_progress(count, block_size, total_size):
        percent = int(count * block_size * 100 / total_size)

        if percent % 10 == 0 and percent not in reported:
            reported.append(percent)
            logging.info(f'{file_path}: {percent}%')

    # finally retrieve
    urlretrieve(file_info.url, file_path, reporthook=_maybe_report_download_progress)

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

def download_with_progress_bar(url: str, file_path: Path) -> None:
    try:
        from tqdm import tqdm
        TQDM_AVAILABLE = True
    except ImportError:
        TQDM_AVAILABLE = False

    if not TQDM_AVAILABLE:
        raise ImportError('tqdm is not installed')

    import requests
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
) -> Path | None:
    """Download the closest snapshot for a given block number.

    Args:
        target_path (Path): The path where the snapshot will be saved.
        block_number (int): The target block number.
        network (str, optional): The network ('mainnet' or 'testnet'). Defaults to 'mainnet'.
        progress (bool, optional): Whether to show a progress bar. Defaults to False.
        force_download (bool, optional): Whether to force download even if the file exists. Defaults to False.

    Returns:
        Path | None: The path to the downloaded snapshot, or None if not found.
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
