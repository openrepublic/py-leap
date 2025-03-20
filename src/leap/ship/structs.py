from __future__ import annotations
from msgspec import (
    Struct,
    to_builtins
)


class StateHistoryArgs(Struct, frozen=True):
    endpoint: str
    start_block_num: int
    end_block_num: int = (2 ** 32) - 2
    max_messages_in_flight: int = 4000
    max_message_size: int = 512 * 1024 * 1024
    irreversible_only: bool = False
    fetch_block: bool = True
    fetch_traces: bool = True
    fetch_deltas: bool = True

    contracts: dict[str, bytes] = {}
    action_whitelist: dict[str, list[str]] | None = {}
    delta_whitelist: dict[str, list[str]] | None = {}

    backend_kwargs: dict = {}

    def as_msg(self):
        return to_builtins(self)

    @classmethod
    def from_msg(cls, msg: dict) -> StateHistoryArgs:
        if isinstance(msg, StateHistoryArgs):
            return msg

        return StateHistoryArgs(**msg)
