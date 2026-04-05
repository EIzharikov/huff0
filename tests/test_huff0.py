import os

import pytest

import huff0.huf0 as huf0


def test_roundtrip_repetitive():
    data = b"hello " * 50000
    assert huf0.decompress(huf0.compress(data)) == data


def test_roundtrip_random():
    data = os.urandom(256 * 1024)
    assert huf0.decompress(huf0.compress(data)) == data


def test_roundtrip_rle():
    data = b"\x7f" * 100000
    assert huf0.decompress(huf0.compress(data)) == data


def test_roundtrip_empty():
    data = b""
    assert huf0.decompress(huf0.compress(data)) == data


def test_compress_bound():
    assert huf0.compress_bound(1024 * 1024) > 1024 * 1024


def test_invalid_data():
    with pytest.raises((ValueError, RuntimeError)):
        huf0.decompress(b"not valid data")
