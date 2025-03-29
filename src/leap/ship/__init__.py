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
import platform
from typing import AsyncContextManager
from contextlib import asynccontextmanager as acm

from leap.ship.structs import StateHistoryArgs

from leap.ship._generic import (
    open_state_history as _generic_open_state_history
)

match platform.system():
    case 'Linux':
        from leap.ship._linux import (
            open_state_history as _linux_open_state_history
        )


_backend = os.environ.get('SHIP_BACKEND', 'default')


def set_ship_backend(backend: str):
    global _backend
    _backend = backend


def get_ship_backend() -> str:
    return _backend


def get_ship_provider() -> AsyncContextManager:
    match (platform.system(), get_ship_backend()):
        case ('Linux', 'linux' | 'default'):
            return _linux_open_state_history

        case _:
            return _generic_open_state_history


@acm
async def open_state_history(sh_args: StateHistoryArgs):

    sh_args = StateHistoryArgs.from_dict(sh_args)

    if sh_args.backend:
        set_ship_backend(sh_args.backend)

    async with (
        get_ship_provider()(sh_args)
    ) as provider:
        yield provider
