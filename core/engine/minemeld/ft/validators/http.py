import json
import os.path
from typing import (
    TYPE_CHECKING, Optional, Dict, Any
)

from jsonschema import Draft7Validator

from . import ValidatorResult
from .basepoller import validator as basepoller_validator

JSON_SCHEMA = None
def validator(newconfig: Dict[str, Any], oldconfig: Optional[Dict[str, Any]] = None) -> ValidatorResult:
    result: ValidatorResult = basepoller_validator(newconfig, oldconfig)

    global JSON_SCHEMA

    if JSON_SCHEMA is None:
        with open(os.path.join(os.path.dirname(__file__), 'http.schema.json'), 'r') as f:
            JSON_SCHEMA = json.load(f)

    v = Draft7Validator(JSON_SCHEMA)
    for e in v.iter_errors(newconfig):
        result['errors'].append(str(e))

    if len(result['errors']) != 0 or oldconfig is None:
        return result

    for k in ['url', 'ignore_regex', 'indicator', 'fields', 'encoding']:
        result['requires_reinit'] = result['requires_reinit'] or newconfig.get(k, None) != oldconfig.get(k, None)

    return result
