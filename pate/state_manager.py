import os
import random
import string
from datetime import datetime


class StateManager(object):
    def finished(self, task_id):
        raise NotImplemented()

    def finish(self, task_id):
        raise NotImplemented()


class NoopStateManager(StateManager):
    def finished(self, task_id):
        return False

    def finish(self, task_id):
        pass


class FileStateManager(StateManager):
    def __init__(self, dirname=None):
        self.dirname = dirname or ''.join(random.choice(string.ascii_letters) for _ in range(8))
        self.state = dict()

        os.makedirs(self.dirname, exist_ok=True)
        try:
            with open(os.path.join(self.dirname, "state.txt"), 'r') as f:
                for line in f.readlines():
                    elements = line.strip().split('\t')
                    timestamp = elements[0]
                    task_id = elements[1]
                    self.state[task_id] = timestamp
        except FileNotFoundError:
            pass

    def finished(self, task):
        return str(task.id) in self.state

    def finish(self, task):
        if not self.finished(task):
            timestamp = datetime.utcnow().isoformat(sep='T', timespec='milliseconds')
            self.state[str(task.id)] = timestamp
            with open(os.path.join(self.dirname, "state.txt"), 'a') as f:
                f.write("%s\t%s\n" % (timestamp, str(task.id)))
