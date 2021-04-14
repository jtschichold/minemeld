import logging

from .base import BaseFT, Message
from . import MetadataResult, NodeType


LOG = logging.getLogger(__name__)


class Miner(BaseFT):
    def generate(self):
        for i in range(1000000):
            self.publish_update(
                indicator=f'{self.name}:{i}',
                value={
                    'type': 'random'
                }
            )
        LOG.info(f'{self.name} - done')

    def receive(self, msg: Message) -> bool:
        result = super().receive(msg)
        if result:
            return result

        if msg['method'] == 'generate':
            self.generate()
            return True

        return False

    def start(self) -> None:
        super().start()
        self.msg_queue.put(1, item={
            'async_answer': None,
            'source': None,
            'method': 'generate',
            'args': {}
        })

    @staticmethod
    def get_metadata() -> MetadataResult:
        return {'node_type': NodeType.MINER}


class Processor(BaseFT):
    def receive(self, msg: Message) -> bool:
        result = super().receive(msg)
        if result:
            return result

        if msg['method'] == 'update':
            self.statistics['update.rx'] += 1
            self.publish_update(msg['args']['indicator'], msg['args']['value'])
            return True

        if msg['method'] == 'withdraw':
            self.statistics['withdraw.rx'] += 1
            self.publish_withdraw(msg['args']['indicator'], msg['args']['value'])
            return True

        return False

    @staticmethod
    def get_metadata() -> MetadataResult:
        return {'node_type': NodeType.PROCESSOR}


class Output(BaseFT):
    def receive(self, msg: Message) -> bool:
        result = super().receive(msg)
        if result:
            return result

        if msg['method'] == 'update':
            self.statistics['update.rx'] += 1
            return True

        if msg['method'] == 'withdraw':
            self.statistics['withdraw.rx'] += 1
            return True

        return False

    @staticmethod
    def get_metadata() -> MetadataResult:
        return {'node_type': NodeType.OUTPUT}
