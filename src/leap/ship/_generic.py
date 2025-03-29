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
import time
import logging
from contextlib import asynccontextmanager as acm

import trio
import msgspec
import antelope_rs
from trio_websocket import open_websocket_url

from leap.sugar import LeapJSONEncoder
from leap.ship.decoder import BlockDecoder
from leap.ship.structs import (
    StateHistoryArgs,
    GetStatusRequestV0,
    GetBlocksRequestV0,
    GetBlocksResultV0,
    GetBlocksAckRequestV0,
    Block,
    BlockHeader
)
from leap.ship._benchmark import BenchmarkedBlockReceiver


def decode_block_result(
    result: GetBlocksResultV0,
    decoder: BlockDecoder
) -> Block:
    block_num = result.this_block.block_num

    final_block: dict = BlockHeader.from_block(result).to_dict()

    try:
        if result.block:
            sblock = antelope_rs.abi_unpack(
                'std',
                'signed_block',
                result.block
            )
            final_block['block'] = sblock

        if result.traces:
            mp_traces = decoder.decode_traces(result.traces)
            final_block['traces'] = msgspec.msgpack.decode(mp_traces)

        if result.deltas:
            mp_deltas = decoder.decode_deltas(result.deltas)
            final_block['deltas'] = msgspec.msgpack.decode(mp_deltas)

        return msgspec.convert(final_block, type=Block)

    except Exception as e:
        e.add_note(f'while decoding block {block_num}')
        e.add_note(
            json.dumps(
                final_block,
                indent=4,
                cls=LeapJSONEncoder
            )
        )

        raise e


class BlockReceiver(BenchmarkedBlockReceiver):
    async def _iterator(self):
        async for batch in self._rchan:
            self._maybe_benchmark(len(batch))

            if self._args.output_batched:
                yield batch

            else:
                yield batch[0]


@acm
async def open_state_history(sh_args: StateHistoryArgs):
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
            with send_chan:
                while block_num != sh_args.end_block_num - 1:
                    # receive get_blocks_result
                    result_bytes = await ws.get_message()
                    result = antelope_rs.abi_unpack('std', 'result', result_bytes)
                    block = decode_block_result(
                        msgspec.convert(result, type=GetBlocksResultV0),
                        decoder,
                    )
                    block_num = block.this_block.block_num

                    await send_chan.send([block])

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

        yield BlockReceiver(recv_chan, sh_args)
