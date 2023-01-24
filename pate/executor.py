import logging
import threading
import time
from collections import defaultdict
from pate.exceptions import FailedDependencyError

LOG = logging.getLogger(__name__)


class LocalExecutor:
    def __init__(self, state_manager, max_workers):
        self.max_workers = max_workers
        self.num_workers = 0
        self.state_manager = state_manager
        self.backlog = []
        self.queue = []
        self.wakeup_queue = []
        self.task_waiters = defaultdict(list)
        self.waiters_events = dict()
        self.waiters_count = defaultdict(int)
        self.lock = threading.RLock()

    def submit_task(self, task):
        task.executor = self
        with self.lock:
            self.backlog.append(task)

    def start_task(self, task):
        if not self.state_manager.finished(task):
            LOG.info("starting %s", task.id)
            task.started = True
            task.start()
        else:
            task.finished = True
            LOG.info("skipping %s", task.id)

    def finish_task(self, task):
        with self.lock:
            if task.failed:
                self.fail_task(task)
            for waiter in self.task_waiters[task.id]:
                self.waiters_count[waiter.id] -= 1
                if self.waiters_count[waiter.id] == 0:
                    self.wakeup_queue.append(waiter.id)
            del self.task_waiters[task.id]
            if not task.failed and not self.state_manager.finished(task):
                self.state_manager.finish(task)
                LOG.info("success %s", task.id)

    def fail_task(self, task):
        LOG.error("failed %s: %s", task.id, ' '.join(map(str, task.exception.args)))

    def wait(self, waiter, *tasks):
        with self.lock:
            tasks = [task for task in tasks if not task.finished]
            if not tasks:
                return

            event = threading.Event()
            self.waiters_events[waiter.id] = event
            self.waiters_count[waiter.id] = len(tasks)
            for task in tasks:
                self.task_waiters[task.id].append(waiter)
            self.num_workers -= 1

            LOG.info("waiting %s for %s", waiter.id, [task.id for task in tasks])

        event.wait()

        for task in tasks:
            if task.failed:
                raise FailedDependencyError("failed dependency", task.id)

    def process_queue(self):
        with self.lock:
            changed = True
            while changed:
                changed = False
                LOG.debug("num_workers %d, queue %d, backlog %d" % (self.num_workers, len(self.queue), len(self.backlog)))

                while len(self.wakeup_queue) > 0 and (self.num_workers < self.max_workers):
                    waiter_task_id = self.wakeup_queue.pop()
                    LOG.info("awaken %s", waiter_task_id)
                    self.waiters_events[waiter_task_id].set()
                    self.num_workers += 1

                next_queue = []
                for task in self.queue:
                    if task.finished:
                        self.finish_task(task)
                        self.num_workers -= 1
                        changed = True
                    elif task.started:
                        next_queue.append(task)
                    else:
                        next_queue.append(task)
                        if self.num_workers < self.max_workers:
                            self.start_task(task)
                            self.num_workers += 1
                            changed = True

                # tasks in backlog maybe moved to queue when they don't have unfinished dependencies
                # or can be failed because of failed dependency
                next_backlog = []
                for task in self.backlog:
                    if all(dep.finished and not dep.failed for dep in task.dependencies):
                        next_queue.append(task)
                        changed = True
                    elif any(dep.failed for dep in task.dependencies):
                        task.failed = True
                        task.exception = FailedDependencyError("failed dependency",
                                                               *(dep.id for dep in task.dependencies if dep.failed))
                        self.finish_task(task)
                        changed = True

                    else:
                        next_backlog.append(task)

                self.queue = next_queue
                self.backlog = next_backlog

    def start(self):
        tick = 0
        while True:
            self.process_queue()
            if len(self.queue) == 0:
                break
            time.sleep(1)
            tick += 1
        LOG.info("execution finished")

