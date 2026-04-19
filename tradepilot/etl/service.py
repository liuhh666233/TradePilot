"""Placeholder service contract for the generic ETL foundation."""

from __future__ import annotations

from tradepilot.etl.models import IngestionRequest


class ETLService:
    """Placeholder application service for future dataset orchestration."""

    def run_dataset_sync(self, dataset_name: str, request: IngestionRequest) -> dict:
        """Run one dataset sync when orchestration is implemented."""

        raise NotImplementedError("Stage A only defines the service contract")

    def run_multi_dataset_sync(
        self,
        dataset_names: list[str],
        request: IngestionRequest,
    ) -> dict:
        """Run a multi-dataset sync when orchestration is implemented."""

        raise NotImplementedError("Stage A only defines the service contract")

    def run_bootstrap(self, profile_name: str) -> dict:
        """Run a bootstrap profile when orchestration is implemented."""

        raise NotImplementedError("Stage A only defines the service contract")

    def list_runs(self, dataset_name: str | None = None) -> list[dict]:
        """List run history when metadata queries are implemented."""

        raise NotImplementedError("Stage A only defines the service contract")

    def list_validation_results(
        self,
        dataset_name: str | None = None,
        run_id: int | None = None,
    ) -> list[dict]:
        """List validation results when metadata queries are implemented."""

        raise NotImplementedError("Stage A only defines the service contract")
