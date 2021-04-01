from .base import BaseFT, Message


class Miner(BaseFT):
    def generate(self):
        for i in range(2000):
            self.publish_update(
                indicator=i,
                value={
                    'type': 'number'
                }
            )

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
