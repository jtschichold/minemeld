import json
import os.path
from typing import (
    TYPE_CHECKING, Optional, Dict, Any
)

from jsonschema import Draft7Validator

from . import ValidatorResult
from .base import validator as base_validator
from ..utils import parse_age_out

JSON_SCHEMA = None
def validator(newconfig: Dict[str, Any], oldconfig: Optional[Dict[str, Any]] = None) -> ValidatorResult:
    result: ValidatorResult = base_validator(newconfig, oldconfig)

    global JSON_SCHEMA

    if JSON_SCHEMA is None:
        with open(os.path.join(os.path.dirname(__file__), 'basepoller.schema.json'), 'r') as f:
            JSON_SCHEMA = json.load(f)

    v = Draft7Validator(JSON_SCHEMA)
    for e in v.iter_errors(newconfig):
        result['errors'].append(str(e))

    if (age_out := newconfig.get('age_out')) is not None:
        if (default := age_out.get('default')) is not None:
            try:
                parse_age_out(default)
            except Exception as e:
                result['errors'].append('Invalid default age_out')

    if len(result['errors']) != 0 or oldconfig is None:
        return result

    for k in ['source_name', 'attributes', 'age_out']:
        result['requires_reinit'] = result['requires_reinit'] or newconfig.get(k, None) != oldconfig.get(k, None)

    return result
