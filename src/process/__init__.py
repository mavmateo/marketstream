
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .jobs.stocks_cleaning_job import StocksCleaningJob
    from .jobs.crypto_cleaning_job import CryptoCleaningJob

def __getattr__(name: str):
    _map = {
        "StocksCleaningJob": (".jobs.stocks_cleaning_job", "StocksCleaningJob"),
        "CryptoCleaningJob": (".jobs.crypto_cleaning_job", "CryptoCleaningJob"),
    }
    if name in _map:
        import importlib
        module_path, class_name = _map[name]
        module = importlib.import_module(module_path, package=__name__)
        return getattr(module, class_name)
    raise AttributeError(f"module 'process' has no attribute {name!r}")

__all__ = ["StocksCleaningJob", "CryptoCleaningJob"]