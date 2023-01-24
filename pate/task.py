import threading
import logging

from pate.exceptions import FailedDependencyError

LOG = logging.getLogger(__name__)


class TaskID(object):
    def __init__(self, prefix=None, name=None):
        self.prefix = prefix or ""
        self.name = name or ""

    def __str__(self):
        return str(self.prefix) + "/" + str(self.name)

    def __repr__(self):
        return str(self.prefix) + "/" + str(self.name)

    def __truediv__(self, name):
        return TaskID(prefix=str(self), name=name)


class Task(threading.Thread):
    def __init__(self):
        super(Task, self).__init__()
        self.id = None
        self.parent = None
        self.started = False
        self.finished = False
        self.failed = False
        self.exception = None
        self.dependencies = []
        self.children = []
        self.executor = None

    def depends(self, *tasks):
        assert not self.started
        for task in tasks:
            self.dependencies.append(task)
        return self

    def submit(self, parent, executor, id):
        self.parent = parent
        self.executor = executor

        if isinstance(id, TaskID):
            self.id = id
        else:
            self.id = TaskID(prefix=None, name=str(id))

        if parent:
            self.parent.children.append(self)

        self.executor.submit_task(self)
        return self

    def wait(self):
        if self.parent:
            self.executor.wait(self.parent, self)

        return self

    def __call__(self, executor):
        pass

    def run(self):
        try:
            self.__call__(self.executor)
            self.executor.wait(self, *self.children)
            self.finished = True
        except FailedDependencyError as e:
            self.exception = e
            self.finished = True
            self.failed = True
        except Exception as e:
            self.exception = e
            self.finished = True
            self.failed = True
            LOG.exception(e)


class Wrapper(Task):
    def __init__(self, func):
        super(Wrapper, self).__init__()
        self.func = func

    def __call__(self, executor):
        self.func()
