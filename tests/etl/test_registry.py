"""Contract tests for the ETL dataset registry."""

from __future__ import annotations

import unittest

from tradepilot.etl.datasets import DatasetDefinition
from tradepilot.etl.models import DatasetCategory, StorageZone
from tradepilot.etl.registry import DatasetRegistry


class DatasetRegistryTests(unittest.TestCase):
    """Verify in-memory dataset registry behavior."""

    def setUp(self) -> None:
        """Create a fresh registry for each test."""

        self.registry = DatasetRegistry()
        self.definition = DatasetDefinition(
            dataset_name="market.etf_daily",
            category=DatasetCategory.MARKET,
            grain="daily",
            primary_source="tushare",
            storage_zone=StorageZone.RAW,
        )

    def test_register_and_lookup_dataset(self) -> None:
        """Register one dataset and look it up by name."""

        self.registry.register_dataset(self.definition)

        self.assertTrue(self.registry.has_dataset("market.etf_daily"))
        self.assertEqual(self.registry.get_dataset("market.etf_daily"), self.definition)
        self.assertEqual(self.registry.list_datasets(), [self.definition])

    def test_register_rejects_duplicate_dataset_name(self) -> None:
        """Reject duplicate dataset names."""

        self.registry.register_dataset(self.definition)

        with self.assertRaisesRegex(ValueError, "dataset already registered"):
            self.registry.register_dataset(self.definition)

    def test_get_dataset_raises_for_unknown_dataset(self) -> None:
        """Fail clearly when a dataset is missing."""

        with self.assertRaisesRegex(KeyError, "unknown dataset"):
            self.registry.get_dataset("missing.dataset")


if __name__ == "__main__":
    unittest.main()
