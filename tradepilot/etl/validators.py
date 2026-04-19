"""Validation contracts for the generic ETL foundation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

from tradepilot.etl.models import ValidationResultRecord


class ValidationRuleDefinition(BaseModel):
    """Lightweight metadata for one validation rule."""

    rule_name: str
    level: str
    description: str | None = None


class BaseValidator(ABC):
    """Base interface for dataset validators."""

    @abstractmethod
    def validate(
        self,
        payload: Any,
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResultRecord]:
        """Validate a payload and return structured validation results."""

        raise NotImplementedError
