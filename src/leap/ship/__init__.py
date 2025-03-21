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


_SHIP_BACKEND = os.environ.get('SHIP_BACKEND', 'default')


match platform.system():
    case 'Linux' if _SHIP_BACKEND != 'generic':
        from leap.ship._linux import (
            open_state_history as open_state_history
        )

    case _:
        from leap.ship._generic import (
            open_state_history as open_state_history
        )
