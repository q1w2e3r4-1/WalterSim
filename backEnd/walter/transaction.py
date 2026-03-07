import uuid


class Transaction:
    def __init__(self):
        self.updates = []

    def create(self, committedVTS):
        self.tid = str(uuid.uuid4())     # 生成一个随机id
        self.startVTS = committedVTS[:]  # 开始时候的时间向量

    def __str__(self):
        return (
            "Transaction(\ntid={}, \nstartVTS={},\nupdates={}\n)"
            .format(self.tid, self.startVTS, self.updates)
        )