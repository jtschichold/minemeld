from typing import (
    TypedDict, List
)

ValidatorResult = TypedDict('ValidatorResult', {
    'errors': List[str],
    'requires_reinit': bool,
}, total=False)