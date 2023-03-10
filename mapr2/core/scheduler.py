import logging
from threading import Event, Thread
import time

from mapr2.common.client import WorkerRPCClient
from mapr2.common.consts import MAPPER, REDUCER

LOG = logging.getLogger(__name__)


class Scheduler(Thread):
    """
    Thread that keeps running and schedule tasks on idle workers.
    It gets the task input from processor in a queue.
    """

    def __init__(self, worker_registry, task_store, input_queue, global_lock):
        super().__init__()
        self._worker_registry = worker_registry
        self._ip_queue = input_queue
        self._tstore = task_store
        self._stop = Event()
        self._stop_scheduling = Event()
        self.daemon = True
        self._global_lock = global_lock

    def _send_task_to_worker(self, task, worker):
        address = worker.address

        def _send():
            client = WorkerRPCClient(address[0], address[1])
            if task.get_type() == MAPPER:
                client.send_mapper(task.get_id(), task.get_task_file())
            elif task.get_type() == REDUCER:
                client.send_reducer(task.get_id(), task.get_all_intermediate_files())
            else:
                LOG.debug(
                    "Unknown task type {}:{}".format(task.get_id(), task.get_type())
                )
                return

        failure_count = 0
        retry_count = 2
        sleep_interval = 3
        success = False
        while failure_count < retry_count:
            failure_count += 1
            try:
                _send()
                LOG.debug(
                    "Submitted job {}:{} to woker {}:{} successfully".format(
                        task.get_id(), task.get_type(), worker.worker_id, address
                    )
                )
                success = True
                break
            except Exception as ex:
                LOG.debug(
                    "Submition of job {}:{} to woker {}:{} failed due to {}, trying again".format(
                        task.get_id(), task.get_type(), worker.worker_id, address, ex
                    )
                )
                time.sleep(sleep_interval)
        if not success:
            with self._global_lock:
                task.reset()

    def stop(self):
        LOG.debug("Scheduler Recieved stop signal")
        self._stop.set()

    def _is_reducer_valid(self, task):
        return len(task.get_all_intermediate_files()) != 0

    def run(self):
        LOG.debug("Scheduler Starting")
        while not self._stop.wait(0.3):
            with self._global_lock:
                idle_worker = self._worker_registry.get_idle_server()
            if idle_worker:
                valid_task = None
                while not valid_task:
                    task = self._ip_queue.get()
                    if task.get_type() == REDUCER:
                        if not self._is_reducer_valid(task):
                            LOG.debug(
                                "Dropping Reducer task {} as it does not have any intermediate files to work on".format(
                                    task.get_id()
                                )
                            )
                            continue
                    valid_task = task
                with self._global_lock:
                    valid_task.mark_processing()
                    valid_task.set_start_time()
                    valid_task.set_worker(idle_worker.worker_id)
                    self._worker_registry.mark_busy(idle_worker.worker_id)
                thread = Thread(
                    target=self._send_task_to_worker, args=(valid_task, idle_worker)
                )
                thread.start()
        LOG.debug("Scheduler Exiting")
