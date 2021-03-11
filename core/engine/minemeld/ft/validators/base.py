import json
import os.path
from typing import (
    TYPE_CHECKING, Optional, Dict, Any
)

from jsonschema import Draft7Validator

from . import ValidatorResult
from ..filters import Filters

JSON_SCHEMA = None
def validator(newconfig: Dict[str, Any], oldconfig: Optional[Dict[str, Any]] = None) -> ValidatorResult:
    result: ValidatorResult = {}

    global JSON_SCHEMA

    if JSON_SCHEMA is None:
        with open(os.path.join(os.path.dirname(__file__), 'base.schema.json'), 'r') as f:
            JSON_SCHEMA = json.load(f)

    v = Draft7Validator(JSON_SCHEMA)
    result['errors'] = [str(e) for e in v.iter_errors(newconfig)]

    for k, v in newconfig.items():
        if k in ['infilters', 'outfilters']:
            try:
                Filters(v)
            except Exception as e:
                result['errors'].append(f'{k}: {str(e)}')

    if len(result['errors']) != 0 or oldconfig is None:
        return result

    result['requires_reinit'] = False
    for k in ['infilters', 'outfilters']:
        result['requires_reinit'] = result['requires_reinit'] or newconfig.get(k, None) != oldconfig.get(k, None)

    return result
