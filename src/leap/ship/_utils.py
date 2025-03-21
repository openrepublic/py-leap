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
