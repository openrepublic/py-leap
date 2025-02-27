import os

package_dir = os.path.dirname(__file__)

std_abi_file_path = os.path.join(package_dir, 'std_abi.json')
std_eosio_file_path = os.path.join(package_dir, 'eosio.json')


def load_std_eosio_abi() -> bytes:
    with open(std_eosio_file_path, 'rb') as file:
        return file.read()

def load_std_abi() -> bytes:
    with open(std_abi_file_path, 'rb') as file:
        return file.read()


STD_ABI = load_std_abi()
STD_EOSIO_ABI = load_std_eosio_abi()
