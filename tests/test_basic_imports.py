import importlib


def test_icechunk_available():
    """Ensure the icechunk package can be imported."""
    module = importlib.import_module("icechunk")
    assert module is not None


def test_actions_package_available():
    """Ensure our actions_package can be imported."""
    module = importlib.import_module("actions_package")
    assert module is not None
