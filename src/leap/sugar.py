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
    target_path: Path, network='telos', version='v6',
    user_agent: str = 'py-leap: python antelope client'
):
    import requests
    from urllib.request import ProxyHandler, urlretrieve, build_opener, install_opener

    proxy = ProxyHandler({})
    opener = build_opener(proxy)
    opener.addheaders = [
        ('User-Agent', user_agent)
    ]
    install_opener(opener)
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
    network: str = 'Telos Mainnet - v6',
    progress: bool = False,
    force_download: bool = False,
    user_agent: str = 'py-leap: python antelope client'
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
    import requests

    from urllib.request import ProxyHandler, urlretrieve, build_opener, install_opener
    from zstandard import ZstdDecompressor

    target_path = Path(target_path)

    proxy = ProxyHandler({})
    opener = build_opener(proxy)
    opener.addheaders = [
        ('User-Agent', user_agent)
    ]
    install_opener(opener)

    _repository_url = 'https://snapshots.eosnation.io/'

    logging.info('Fetching snapshot list...')
    response = requests.get(_repository_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    network_card = None
    for card in soup.find_all('div', class_='card'):
        if network.lower() in card.text.lower():
            network_card = card
            break

    if not network_card:
        raise ValueError('No suitable snapshot found.')

    closest_block = None
    file_url = None
    min_diff = float('inf')

    for li in network_card.find_all('li'):
        link = li.find('a')['href']
        if 'snapshot' in link:
            block_str = link.split('-')[-1].split('.')[0]
            try:
                block_num = int(block_str)
                diff = block_number - block_num
                if diff >= 0 and diff < min_diff:
                    min_diff = diff
                    closest_block = block_num
                    file_url = link
            except ValueError:
                continue

    if not file_url:
        raise ValueError('No suitable snapshot found.')

    # Determine the final .bin file name
    closest_snapshot = file_url.split('/')[-1]
    final_bin_file_name = closest_snapshot.split('.bin.zst')[0] + '.snapshot.bin'
    final_bin_file_path = target_path / final_bin_file_name

    # Check if the .bin file already exists
    if final_bin_file_path.exists() and not force_download:
        logging.info(f'Snapshot file {final_bin_file_name} already exists. Skipping download.')
        return final_bin_file_path

    # Download the file
    file_path = target_path / closest_snapshot
    logging.info(f'Downloading snapshot: {file_url}')

    if progress:
        download_with_progress_bar(file_url, file_path)
    else:
        urlretrieve(file_url, file_path)

    # Decompress the tar.gz
    logging.info(f'Decompressing snapshot to: {final_bin_file_path}')

    with open(file_path, 'rb') as compressed_file:
        dctx = ZstdDecompressor()
        with open(final_bin_file_path, 'wb') as output_file:
            dctx.copy_stream(compressed_file, output_file)

    # Clean up temporary files and directories
    logging.info('Cleaning up temporary files...')
    file_path.unlink()

    logging.info('Operation completed successfully.')
    return final_bin_file_path
