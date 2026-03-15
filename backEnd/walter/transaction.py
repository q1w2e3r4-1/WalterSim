import uuid
from enum import Enum

class Status(Enum):
    ACTIVE = 0
    COMMITTED = 1
    ABORTED = 2

class Transaction:
    def __init__(self):
        self.updates = []

    def create(self, committedVTS):
        self.tid = str(uuid.uuid4())     # 生成一个随机id
        self.startVTS = committedVTS[:]  # 开始时候的时间向量
        self.status = Status.ACTIVE

    def __str__(self):
        return (
            "Transaction(\ntid={}, \nstartVTS={},\nupdates={}\nstatus={}\n)"
            .format(self.tid, self.startVTS, self.updates, self.status)
        )
    
    def add_update(self, update: list):
        self.updates.append(update)

    