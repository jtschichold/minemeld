from typing import (
    TypedDict, List, Protocol, Dict, Optional, Any
)

class ValidatorResult(TypedDict, total=False):
    errors: List[str]
    requires_reinit: bool


class Validator(Protocol):
    @staticmethod
    def get_schema() -> Dict[str, Any]:
        ...

    @staticmethod
    def validate(self, newconfig: Dict[str, Any], oldconfig: Optional[Dict[str, Any]] = None) -> ValidatorResult:
        ...
