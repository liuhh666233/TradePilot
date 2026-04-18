"""ETF all-weather stage-one API routes."""

from fastapi import APIRouter

from tradepilot.etf_all_weather.models import (
    EtfAllWeatherCalendarSyncRequest,
    EtfAllWeatherCurveSyncRequest,
    EtfAllWeatherFeatureSnapshotRequest,
    EtfAllWeatherInitResult,
    EtfAllWeatherJobResult,
    EtfAllWeatherMarketSyncRequest,
    EtfAllWeatherRegimeSnapshotRequest,
    EtfAllWeatherSlowMacroSyncRequest,
)
from tradepilot.etf_all_weather.service import EtfAllWeatherStageOneService

router = APIRouter()
_service = EtfAllWeatherStageOneService()


@router.get("/status")
def get_status() -> dict:
    """Return ETF all-weather stage-one readiness and table counts."""

    return _service.get_status()


@router.post("/init", response_model=EtfAllWeatherInitResult)
def initialize_stage_one() -> EtfAllWeatherInitResult:
    """Apply schema and create the stage-one storage layout."""

    return _service.initialize_schema()


@router.post("/trading-calendar/sync", response_model=EtfAllWeatherJobResult)
def sync_trading_calendar(
    request: EtfAllWeatherCalendarSyncRequest,
) -> EtfAllWeatherJobResult:
    """Sync the canonical trading and rebalance calendars."""

    return _service.sync_trading_calendar(request)


@router.post("/sleeve-daily-market/sync", response_model=EtfAllWeatherJobResult)
def sync_sleeve_daily_market(request: EtfAllWeatherMarketSyncRequest) -> EtfAllWeatherJobResult:
    """Sync the canonical five-sleeve daily ETF market history."""

    return _service.sync_sleeve_daily_market(request)


@router.post("/benchmark-index-daily/sync", response_model=EtfAllWeatherJobResult)
def sync_benchmark_index_daily(request: EtfAllWeatherMarketSyncRequest) -> EtfAllWeatherJobResult:
    """Sync the benchmark index history for market-confirmation inputs."""

    return _service.sync_benchmark_index_daily_market(request)


@router.post("/slow-macro/sync", response_model=EtfAllWeatherJobResult)
def sync_slow_macro(request: EtfAllWeatherSlowMacroSyncRequest) -> EtfAllWeatherJobResult:
    """Sync the v1 primary slow macro field set with timing metadata."""

    return _service.sync_slow_macro(request)


@router.post("/curve/sync", response_model=EtfAllWeatherJobResult)
def sync_curve(request: EtfAllWeatherCurveSyncRequest) -> EtfAllWeatherJobResult:
    """Sync China government curve points with windowed extraction."""

    return _service.sync_curve(request)


@router.post("/monthly-feature-snapshot/build", response_model=EtfAllWeatherJobResult)
def build_monthly_feature_snapshot(request: EtfAllWeatherFeatureSnapshotRequest | None = None) -> EtfAllWeatherJobResult:
    """Build the explainability-ready monthly as-of feature snapshot dataset."""

    if request is None:
        request = EtfAllWeatherFeatureSnapshotRequest()
    return _service.build_monthly_feature_snapshot(request)


@router.post("/monthly-regime-snapshot/build", response_model=EtfAllWeatherJobResult)
def build_monthly_regime_snapshot(request: EtfAllWeatherRegimeSnapshotRequest | None = None) -> EtfAllWeatherJobResult:
    """Build rule-based regime, confidence, and target budgets."""

    if request is None:
        request = EtfAllWeatherRegimeSnapshotRequest()
    return _service.build_monthly_regime_snapshot(request)
