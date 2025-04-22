# py-leap: Antelope protocol framework
# Copyright 2021-eternity Guillermo Rodriguez

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import os
from pathlib import Path

from leap.protocol import ABI


os.environ['RUST_BACKTRACE'] = '1'


def _load_abi_file(p: Path) -> str:
    with open(p, 'r') as file:
        return file.read()

package_dir = Path(__file__).parent


ABI_PATHS: dict[str, Path] = {
    'std': package_dir / 'std_abi.json',
    'eosio': package_dir / 'eosio.json',
    'eosio.token': package_dir / 'eosio.token.json',
}


ABI_STRINGS: dict[str, str] = {
    account: _load_abi_file(abi_path)
    for account, abi_path in ABI_PATHS.items()
}


ABIS: dict[str, ABI] = {
    account: ABI.from_str(abi_str)
    for account, abi_str in ABI_STRINGS.items()
}


standard = ABIS['std']
system = ABIS['eosio']
token = ABIS['eosio.token']
