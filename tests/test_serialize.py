import logging
import antelope_rs

from leap.abis import STD_ABI


def test_ship():
    antelope_rs.load_abi('std', STD_ABI)

    antelope_rs.abi_pack('std', 'request', ['get_status_request_v0', {}])

    request = [
        'get_blocks_request_v0', 
        {
            'start_block_num': 420,
            'end_block_num': 470,
            'max_messages_in_flight': 1000,
            'have_positions': [],
            'irreversible_only': False,
            'fetch_block': True,
            'fetch_traces': True,
            'fetch_deltas': True
        }
    ]
    logging.info(f"request: {request}")

    packed = antelope_rs.abi_pack('std', 'request', request)
    logging.info(f"packed: {packed}")

    unpacked = antelope_rs.abi_unpack('std', 'request', packed)
    logging.info(f"unpacked: {unpacked}")

    assert request == unpacked


def test_ship_result():
    antelope_rs.load_abi('std', STD_ABI)

    result_raw = bytes.fromhex('011700000000000017d1359487a1d12277aec6a0d50207a7fa3a46a0b18ad11a6a093594b4150000000000001522ebe6ddc1f00b426e69faa006026c7dcf59815d0282cb579c8e21d1010a0000000000000ac54a7ca25f05f01a1caa35040d73e8a1298fbfadc9ad3bf18694243a010900000000000009485ad52d4d4d387b27c562a911790007288231fc107f80c77ebee29b01b801be92725e0000000000ea3055000000000009485ad52d4d4d387b27c562a911790007288231fc107f80c77ebee29b0000000000000000000000000000000000000000000000000000000000000000d007eefcb4c20a0ee78d2dfe8446527d91ab984240edea8fc5a7062100e4dedb00000000000000202ea20c87518fc9bfa42deb7bb1791e81657cca8c52f30042960690b480cd067e2b454c73eebda1087394fbffc9a606043fd89f8a4601abeedebd853873e5c9fa00000000')
    result_type, result = antelope_rs.abi_unpack('std', 'result', result_raw)

    assert result_type == 'get_blocks_result_v0'
    assert isinstance(result, dict)
