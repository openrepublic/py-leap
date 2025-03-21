import tempfile

from leap.protocol import Asset
from leap.tokens import tlos_token

from leap.sugar import download_snapshot


def test_asset_to_str():
    assert str(Asset(1000 * (10 ** 4), tlos_token)) == '1000.0000 TLOS'

def test_asset_from_str():
    assert Asset(1000 * (10 ** 4), tlos_token) == Asset.from_str('1000.0000 TLOS')

def test_zero_prec_asset():
    assert str(Asset(1, '0,SYS')) == '1 SYS'

def test_snap_download():
    download_snapshot(tempfile.gettempdir(), 1_000_000, progress=True)
