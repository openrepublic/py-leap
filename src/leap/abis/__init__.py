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
import json
from pathlib import Path

import antelope_rs


def _load_abi_file(p: Path) -> bytes:
    with open(p, 'rb') as file:
        return file.read()

package_dir = Path(__file__).parent


ABI_PATHS: dict[str, Path] = {
    'std': package_dir / 'std_abi.json',
    'eosio': package_dir / 'eosio.json',
    'eosio.token': package_dir / 'eosio.token.json',
}


RAW_ABIS: dict[str, bytes] = {
    account: _load_abi_file(abi_path)
    for account, abi_path in ABI_PATHS.items()
}


for account, abi in RAW_ABIS.items():
    antelope_rs.load_abi(account, abi)


ABIS: dict[str, dict] = {
    account: json.loads(abi_raw.decode('utf-8'))
    for account, abi_raw in RAW_ABIS.items()
}
