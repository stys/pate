import time
import pate


class HelloTask(pate.Task):
    def __init__(self):
        super(HelloTask, self).__init__()

    def __call__(self, executor):
        time.sleep(2)


class WordTask(pate.Task):
    def __init__(self):
        super(WordTask, self).__init__()

    def __call__(self, executor):
        pass


class MyBadMainTask(pate.Task):
    def __init__(self):
        super(MyBadMainTask, self).__init__()

    def __call__(self, executor):
        task_hello = HelloTask().\
            submit(self, executor, self.id / "hello").wait()

        task_word = WordTask(). \
            depends(task_hello). \
            submit(self, executor, self.id / "word")

        task_more = pate.Wrapper(func=lambda: 5 / 0). \
            submit(self, executor, self.id / "other")


class MyGoodMainTask(pate.Task):
    def __init__(self):
        super(MyGoodMainTask, self).__init__()

    def __call__(self, executor):
        task_hello = HelloTask().\
            submit(self, executor, self.id / "hello")

        task_word = WordTask(). \
            depends(task_hello). \
            submit(self, executor, self.id / "word").wait()

        task_more = pate.Wrapper(func=lambda: 5 / 1). \
            submit(self, executor, self.id / "other")


class MyFollowUpTask(pate.Task):
    def __init__(self):
        super(MyFollowUpTask, self).__init__()

    def __call__(self, executor):
        pass


def test_executor(tmp_path):
    executor1 = pate.LocalExecutor(pate.FileStateManager(tmp_path), 5)

    main_task = MyBadMainTask()
    main_task.submit(None, executor1, "bad")

    follow_task = MyFollowUpTask().\
        depends(main_task).\
        submit(None, executor1, "follow")

    executor1.start()

    assert follow_task.failed

    executor2 = pate.LocalExecutor(pate.FileStateManager(tmp_path), 5)

    main_task = MyGoodMainTask()
    main_task.submit(None, executor2, "good")

    follow_task = MyFollowUpTask(). \
        depends(main_task). \
        submit(None, executor2, "follow")

    executor2.start()

    assert not follow_task.failed
