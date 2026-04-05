"""Structured data provider factory and exports."""

from tradepilot.config import DATA_PROVIDER, DataProviderType
from tradepilot.data.provider import DataProvider

_provider: DataProvider | None = None


def get_provider() -> DataProvider:
    """Return the configured structured data provider (singleton)."""
    global _provider
    if _provider is not None:
        return _provider
    if DATA_PROVIDER == DataProviderType.AKSHARE:
        from tradepilot.data.akshare_provider import AKShareProvider

        _provider = AKShareProvider()
    elif DATA_PROVIDER == DataProviderType.MOCK:
        from tradepilot.data.mock_provider import MockProvider

        _provider = MockProvider()
    else:
        raise ValueError(f"unsupported DATA_PROVIDER: {DATA_PROVIDER}")
    return _provider


__all__ = [
    "DataProvider",
    "get_provider",
]
