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
from __future__ import annotations
from msgspec import (
    Struct,
    to_builtins
)


class Whitelist(Struct, frozen=True):

    inner: dict[str, list[str]] | None

    def as_msg(self):
        return to_builtins(self.inner)

    @classmethod
    def from_msg(cls, msg: dict) -> Whitelist:
        if isinstance(msg, Whitelist):
            return msg

        return Whitelist(inner=msg)

    def is_relevant(self, obj: any) -> bool:
        if self.inner is None:
            return True

        first, second = obj.whitelist_keys()

        relevant_items = self.inner.get(first, [])
        return (
            '*' in relevant_items
            or
            second in relevant_items
        )
