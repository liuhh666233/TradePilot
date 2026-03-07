"""Collector API: manual sync endpoints for ingestion."""

from fastapi import APIRouter

from tradepilot.ingestion.models import BilibiliSyncRequest, NewsSyncRequest, SyncRequest, SyncResult
from tradepilot.ingestion.service import IngestionService

router = APIRouter()
_service = IngestionService()


@router.post("/market/sync")
def market_sync(request: SyncRequest | None = None) -> SyncResult:
    """Trigger a manual market data sync."""
    if request is None:
        request = SyncRequest()
    return _service.sync_market(request)


@router.post("/news/sync")
def news_sync(request: NewsSyncRequest | None = None) -> SyncResult:
    """Trigger a manual news sync."""
    if request is None:
        request = NewsSyncRequest()
    return _service.sync_news(request)


@router.post("/bilibili/sync")
def bilibili_sync(request: BilibiliSyncRequest | None = None) -> SyncResult:
    """Trigger a manual Bilibili video sync."""
    if request is None:
        request = BilibiliSyncRequest()
    return _service.sync_bilibili(request)


@router.get("/runs")
def list_runs() -> list[dict]:
    """Return ingestion run history."""
    return _service.get_runs()


@router.get("/status")
def ingestion_status() -> dict:
    """Return high-level ingestion status."""
    return _service.get_status()
