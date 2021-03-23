import os.path
import json
from typing import (
    Dict, Any, Optional
)

from . import ValidatorResult
from .base import BaseValidator


class MinerValidator:
    schema: Optional[dict] = None

    @staticmethod
    def get_schema() -> Dict[str, Any]:
        return BaseValidator.get_schema()

    @staticmethod
    def validate(self, newconfig: Dict[str, Any], oldconfig: Optional[Dict[str, Any]] = None) -> ValidatorResult:
        return BaseValidator.validate(newconfig, oldconfig)
