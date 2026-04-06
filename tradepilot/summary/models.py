"""Pydantic v2 models for market summary API responses."""

from pydantic import BaseModel, Field, model_validator


class IndexSnapshot(BaseModel):
    """Real-time snapshot of a single market index."""

    code: str
    name: str
    close: float
    change_pct: float
    change_val: float
    volume: float
    turnover: float


class MarketBreadth(BaseModel):
    """Market-wide up/down/limit statistics."""

    total: int
    up: int
    down: int
    flat: int
    limit_up: int
    limit_up_20: int
    limit_down: int
    limit_down_20: int


class SectorRecord(BaseModel):
    """Single sector (industry or concept) snapshot."""

    code: str
    name: str
    change_pct: float
    up_count: int
    down_count: int
    leader: str
    leader_code: str = ""


class StockRecord(BaseModel):
    """Single stock change snapshot."""

    code: str
    name: str
    change_pct: float


class DailySummaryResponse(BaseModel):
    """Full daily market summary."""

    date: str
    timestamp: str
    indices: list[IndexSnapshot]
    breadth: MarketBreadth
    industry_top: list[SectorRecord]
    industry_bottom: list[SectorRecord]
    concept_top: list[SectorRecord]
    concept_bottom: list[SectorRecord]
    stocks_top: list[StockRecord]
    stocks_bottom: list[StockRecord]


class RegimeInfo(BaseModel):
    """Market regime classification for 5-minute brief."""

    label: str
    score: float
    drivers: dict


class WatchSectorRecord(BaseModel):
    """Watchlist sector with signal classification."""

    name: str
    matched_name: str
    change_pct: float
    up_count: int
    down_count: int
    strength: float
    status: str


class WatchStockRecord(BaseModel):
    """Watchlist stock with signal classification."""

    code: str
    name: str
    price: float
    change_pct: float
    change_val: float
    turnover_rate: float
    volume_ratio: float
    status: str


class FiveMinBriefResponse(BaseModel):
    """Intraday 5-minute brief response."""

    date: str
    timestamp: str
    regime: RegimeInfo
    sector_watchlist: list[WatchSectorRecord]
    stock_watchlist: list[WatchStockRecord]
    alerts: list[str]


class WatchSectorConfig(BaseModel):
    """Watch sector metadata used by workflow builders and news mapping."""

    name: str
    role: str = "watch_sector"
    thesis: str | None = None
    report_aliases: list[str] = Field(default_factory=list)


class WatchStockConfig(BaseModel):
    """Watch stock metadata used by workflow builders and insight context."""

    code: str
    name: str | None = None
    role: str = "watch_stock"
    theme: str | None = None
    thesis: str | None = None
    notes: str | None = None


class WatchGroupConfig(BaseModel):
    """Grouped watch configuration for positions or generic watchlists."""

    sectors: list[WatchSectorConfig] = Field(default_factory=list)
    stocks: list[WatchStockConfig] = Field(default_factory=list)


class WatchlistConfig(BaseModel):
    """Normalized watchlist configuration with backward-compatible loading."""

    positions: WatchGroupConfig = Field(default_factory=WatchGroupConfig)
    watchlist: WatchGroupConfig = Field(default_factory=WatchGroupConfig)

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_shape(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        if "positions" in data or "watchlist" in data:
            return data

        watch_sectors = data.get("watch_sectors", []) or []
        watch_stocks = data.get("watch_stocks", []) or []
        return {
            "positions": {"sectors": [], "stocks": []},
            "watchlist": {
                "sectors": [
                    sector if isinstance(sector, dict) else {"name": str(sector)}
                    for sector in watch_sectors
                ],
                "stocks": watch_stocks,
            },
        }

    @property
    def watch_sectors(self) -> list[str]:
        """Return watch sector names for backward-compatible consumers."""

        return [item.name for item in self.watchlist.sectors]

    @property
    def watch_stocks(self) -> list[dict]:
        """Return watch stock records for backward-compatible consumers."""

        return [item.model_dump() for item in self.watchlist.stocks]

    def to_legacy_dict(self) -> dict:
        """Return the historical flat watchlist shape used by old code paths."""

        return {
            "watch_sectors": self.watch_sectors,
            "watch_stocks": self.watch_stocks,
        }


class TradingStatusResponse(BaseModel):
    """Current A-share trading session status."""

    is_trading: bool
    status: str
    next_open: str | None = None
    message: str
