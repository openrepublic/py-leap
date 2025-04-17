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
import logging
from typing import Callable
from contextlib import asynccontextmanager as acm

import trio
import msgspec
import antelope_rs
from trio_websocket import open_websocket_url

from leap.ship.decoder import BlockDecoder
from leap.ship.structs import (
    StateHistoryArgs,
    GetStatusRequestV0,
    GetBlocksRequestV0,
    GetBlocksResultV0,
    GetBlocksAckRequestV0,
)
from leap.ship._benchmark import BenchmarkedBlockReceiver


class BlockReceiver(BenchmarkedBlockReceiver):
    async def _receiver(self):
        async for batch in self._rchan:
            yield batch


@acm
async def open_state_history(
    sh_args: StateHistoryArgs,
    benchmark_log_fn: Callable | None = None
):
    sh_args = StateHistoryArgs.from_dict(sh_args)

    decoder = BlockDecoder(sh_args)

    send_chan, recv_chan = trio.open_memory_channel(0)

    async with (
        trio.open_nursery() as n,
        open_websocket_url(
            sh_args.endpoint,
            max_message_size=sh_args.max_message_size,
            message_queue_size=sh_args.max_messages_in_flight
        ) as ws
    ):
        async def _receiver():
            # receive blocks & manage acks
            acked_block = sh_args.start_block_num
            block_num = sh_args.start_block_num - 1
            msg_index = 0
            with send_chan:
                while True:
                    # receive get_blocks_result
                    result_bytes = await ws.get_message()
                    result = antelope_rs.abi_unpack('std', 'result', result_bytes)
                    result = msgspec.convert(result, type=GetBlocksResultV0)

                    block = decoder.decode_block_result(result)
                    block['ws_index'] = msg_index
                    block['ws_size'] = len(result_bytes)

                    await send_chan.send([block])

                    msg_index += 1

                    block_num = result.this_block.block_num
                    if block_num == sh_args.end_block_num - 1:
                        break

                    if acked_block == block_num:
                        # ack next batch of messages
                        await ws.send_message(
                            antelope_rs.abi_pack(
                                'std',
                                'request',
                                GetBlocksAckRequestV0(
                                    num_messages=sh_args.max_messages_in_flight
                                ).to_dict()
                            )
                        )

                        acked_block += sh_args.max_messages_in_flight


        # first message is ABI
        _ = await ws.get_message()

        # send get_status_request
        await ws.send_message(
            antelope_rs.abi_pack('std', 'request', GetStatusRequestV0().to_dict()))

        # receive get_status_result
        status_result_bytes = await ws.get_message()
        status = antelope_rs.abi_unpack('std', 'result', status_result_bytes)
        logging.info(status)

        # send get_blocks_request
        get_blocks_msg = antelope_rs.abi_pack(
            'std',
            'request',
            GetBlocksRequestV0(
                start_block_num= sh_args.start_block_num,
                end_block_num= sh_args.end_block_num,
                max_messages_in_flight= sh_args.max_messages_in_flight,
                have_positions= [],
                irreversible_only= sh_args.irreversible_only,
                fetch_block= sh_args.fetch_block,
                fetch_traces= sh_args.fetch_traces,
                fetch_deltas= sh_args.fetch_deltas
            ).to_dict()
        )
        await ws.send_message(get_blocks_msg)

        n.start_soon(_receiver)

        yield BlockReceiver(
            recv_chan,
            sh_args,
            log_fn=benchmark_log_fn
        )
