import logging
from threading import Event, Thread
import time

from mapr2.common.consts import NULL_WORKER_ID, TASK_FINISH_TIMEOUT

LOG = logging.getLogger(__name__)


class TaskProcessor(Thread):
    """
    A thread which checks for idle mappers/reducers and send them to scheduler
    to run on appropriate worker.
    """

    def __init__(
        self, input_queue, task_store, mapper_fin_evt, reducer_fin_evt, global_lock
    ):
        super().__init__()
        self._input_queue = input_queue
        self._tstore = task_store
        self._mapper_finished = mapper_fin_evt
        self._reducer_finished = reducer_fin_evt
        self._stop = Event()
        self._global_lock = global_lock
        self.daemon = True
        self._stopped = False

    def run(self):
        while not self._stop.is_set():
            LOG.debug("Starting Task Processor to process Mappers")
            while not self._mapper_finished.wait(0.5):
                task = self._tstore.find_idle_mapper()
                if task:
                    with self._global_lock:
                        task.mark_scheduled()
                    self._input_queue.put(task)

            LOG.debug("All Mappers Finished, Starting Reducers")
            while not self._reducer_finished.wait(0.5):
                task = self._tstore.find_idle_reducer()
                if task:
                    with self._global_lock:
                        task.mark_scheduled()
                    self._input_queue.put(task)
            LOG.debug("All Reducers Finished")
            time.sleep(0.2)
        LOG.debug("Task Processor Exiting")
        self._stopped = True

    def stop(self):
        LOG.debug("Task Processor Recieved stop signal")
        self._stop.set()
        while not self._stopped:
            time.sleep(0.2)


class TaskTimeoutHandler(Thread):
    """
    A thread which checks for long running tasks and make them idle in
    order to be scheduled somewhere else.
    """

    def __init__(self, task_store, global_lock):
        super().__init__()
        self._tstore = task_store
        self._stop = Event()
        self._global_lock = global_lock
        self.daemon = True

    def run(self):
        LOG.debug("Starting Task Timeout Handler")
        while not self._stop.wait(0.5):
            with self._global_lock:
                running_tasks = self._tstore.list_all_running_tasks()
                for task in running_tasks:
                    if (
                        round(time.time() * 1000) - task.get_start_time()
                        > TASK_FINISH_TIMEOUT
                    ):
                        LOG.debug(
                            "Task {}:{} timedout".format(task.get_id(), task.get_type())
                        )
                        LOG.debug(
                            "Marking task {}:{} Idle".format(
                                task.get_id(), task.get_type()
                            )
                        )
                        task.reset()
        LOG.debug("Timeout handler Exiting")

    def stop(self):
        LOG.debug("Timeout handler Recieved stop signal")
        self._stop.set()
