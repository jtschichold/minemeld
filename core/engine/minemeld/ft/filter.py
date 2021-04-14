from typing import Dict, Any, Optional
import os
import json

from jsonschema import Draft7Validator

from . import ValidateResult
from .base import BaseFT, Message
from .filters import Filters


class FilterNode(BaseFT):
    def configure(self, config: Dict[str, Any]) -> None:
        super().configure(config)

        self.infilters = Filters(config.get('infilters', []))
        self.outfilters = Filters(config.get('outfilters', []))

    def publish_update(self, indicator: str, value: Dict[str, Any]) -> None:
        findicator, fvalue = self.outfilters.apply(
            origin=self.name,
            method='update',
            indicator=indicator,
            value=value
        )
        if findicator is None:
            self.statistics['update.txdrp'] += 1
            return
        assert fvalue is not None
        
        return super().publish_update(findicator, fvalue)

    def publish_withdraw(self, indicator: str, value: Dict[str, Any]) -> None:
        findicator, fvalue = self.outfilters.apply(
            origin=self.name,
            method='withdraw',
            indicator=indicator,
            value=value
        )
        if findicator is None:
            self.statistics['withdraw.txdrp'] += 1
            return
        assert fvalue is not None
        
        return super().publish_withdraw(findicator, fvalue)

    def process_update(self, origin: str, findicator: str, fvalue: Dict[str, Any]) -> None:
        pass

    def on_update(self, origin: str, indicator: str, value: Dict[str, Any]) -> None:
        self.statistics['update.rx'] += 1
        findicator, fvalue = self.infilters.apply(
            origin=origin,
            method='update',
            indicator=indicator,
            value=value
        )
        if findicator is None:
            self.statistics['update.rxdrp'] += 1
            return
        assert fvalue is not None

        self.statistics['updatex.processed'] += 1
        self.process_update(origin, findicator, fvalue)

    def process_withdraw(self, origin: str, findicator: str, fvalue: Dict[str, Any]) -> None:
        pass

    def on_withdraw(self, origin: str, indicator: str, value: Dict[str, Any]) -> None:
        self.statistics['withdraw.rx'] += 1
        findicator, fvalue = self.infilters.apply(
            origin=origin,
            method='withdraw',
            indicator=indicator,
            value=value
        )
        if findicator is None:
            self.statistics['withdraw.rxdrp'] += 1
            return
        assert fvalue is not None

        self.statistics['withdrawx.processed'] += 1
        self.process_withdraw(origin, findicator, fvalue)

    def receive(self, msg: Message) -> bool:
        result = super().receive(msg)
        if result:
            return result

        if msg['method'] == 'update':
            self.statistics['update.rx'] += 1
            assert msg['source'] is not None
            self.on_update(
                origin=msg['source'],
                indicator=msg['args']['indicator'],
                value=msg['args']['value']
            )
            return True

        if msg['method'] == 'withdraw':
            self.statistics['withdraw.rx'] += 1
            assert msg['source'] is not None
            self.on_withdraw(
                origin=msg['source'],
                indicator=msg['args']['indicator'],
                value=msg['args']['value']
            )
            return True

        return False

    @staticmethod
    def get_schema() -> Dict[str, Any]:
        if BaseFT.schema is None:
            with open(os.path.join(os.path.dirname(__file__), 'schemas', 'base.schema.json'), 'r') as f:
                BaseFT.schema = json.load(f)

        return BaseFT.schema

    @staticmethod
    def validate(newconfig: Dict[str, Any], oldconfig: Optional[Dict[str, Any]] = None) -> ValidateResult:
        result: ValidateResult = {}

        v = Draft7Validator(BaseFT.get_schema())
        result['errors'] = [str(e) for e in v.iter_errors(newconfig)]

        if len(result['errors']) != 0 or oldconfig is None:
            return result

        result['requires_reinit'] = False

        return result
