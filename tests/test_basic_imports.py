import importlib


def test_icechunk_available():
    """Ensure the icechunk package can be imported."""
    module = importlib.import_module("icechunk")
    assert module is not None


def test_ice_stream_available():
    """Ensure our ice_stream package can be imported."""
    module = importlib.import_module("ice_stream")
    assert module is not None
