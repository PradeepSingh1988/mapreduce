import logging
from queue import Queue
from threading import Event, Lock

from mapr2.common.consts import FILE_DOWNLOAD_FAILED, MAPPER, REDUCER, TASK_FAILURE
from mapr2.core.heartbeat import HeartbeatMonitor, NodeFailureHandler
from mapr2.core.processor import TaskProcessor, TaskTimeoutHandler
from mapr2.core.registry import WorkerRegistry
from mapr2.core.scheduler import Scheduler
from mapr2.core.task import MapperTask, ReducerTask, TaskStore


LOG = logging.getLogger(__name__)


class CurrentTaskCounter(object):
    __slots__ = ("mappers", "reducers")

    def __init__(self, mappers, reducers):
        self.mappers = mappers
        self.reducers = reducers


class Master(object):
    """
    Master Service which handles all orchetration for a map reduce job.
    """

    def __init__(self, task_files, reducer_count):

        self._wregistry = WorkerRegistry()
        self._tstore = TaskStore()
        self._lock = Lock()
        self._mapper_done_evt = Event()
        self._reducer_done_evt = Event()
        self._task_ip_q = Queue(50)
        self._failed_node_queue = Queue()
        self._scheduler = None
        self._hb_monitor = None
        self._task_processor = None
        self._timeout_handler = None
        self._add_job(task_files, reducer_count)
        self._task_counter = CurrentTaskCounter(len(task_files), reducer_count)
        self._start_threads()

    def _start_threads(self):
        self._start_hb_monitor()
        self._start_scheduler()
        self._start_task_processor()
        self._start_timeout_handler()
        self._start_failed_node_handler()

    def _add_mappers(self, task_files):
        with self._lock:
            for index, file in enumerate(task_files):
                task = MapperTask(str(index), file)
                self._tstore.add_mapper(task)

    def _add_reducers(self, reducer_count):
        with self._lock:
            for i in range(reducer_count):
                task = ReducerTask(str(i))
                self._tstore.add_reducer(task)

    def _add_job(self, task_files, reducer_count):
        self._add_mappers(task_files)
        self._add_reducers(reducer_count)

    def register(self, worker_id, address):
        worker = self._wregistry.register(worker_id, address)
        LOG.debug(
            "Worker {}:{} registered successfully".format(
                worker.worker_id, worker.address
            )
        )
        with self._lock:
            return self._tstore.get_no_of_reducers()

    def _start_hb_monitor(self):
        LOG.debug("Starting Heartbet Monitor")
        self._hb_monitor = HeartbeatMonitor(
            self._wregistry,
            self._failed_node_queue,
        )
        self._hb_monitor.start()

    def _start_failed_node_handler(self):
        LOG.debug("Starting Node Failure Handler")
        self._node_failure_handler = NodeFailureHandler(
            self._tstore,
            self._failed_node_queue,
            self._mapper_done_evt,
            self._lock,
            self._task_counter,
        )
        self._node_failure_handler.start()

    def _start_scheduler(self):
        self._scheduler = Scheduler(
            self._wregistry, self._tstore, self._task_ip_q, self._lock
        )
        self._scheduler.start()

    def _start_task_processor(self):
        self._task_processor = TaskProcessor(
            self._task_ip_q,
            self._tstore,
            self._mapper_done_evt,
            self._reducer_done_evt,
            self._lock,
        )
        self._task_processor.start()

    def _start_timeout_handler(self):
        self._timeout_handler = TaskTimeoutHandler(self._tstore, self._lock)
        self._timeout_handler.start()

    def _is_done(self):
        with self._lock:
            if self._task_counter.mappers == 0:
                self._mapper_done_evt.set()
            if self._task_counter.reducers == 0:
                self._reducer_done_evt.set()
            if self._task_counter.mappers == 0 and self._task_counter.reducers == 0:
                self._scheduler.stop()
                self._task_processor.stop()
                self._timeout_handler.stop()
                self._hb_monitor.stop()
                self._node_failure_handler.stop()
                return True
            return False

    def update_task_done(self, task_id, task_type, worker_id, result=None):
        LOG.debug(
            "Recieved job response from worker {} for task {}:{} with result:{}".format(
                worker_id, task_id, task_type, result
            )
        )
        with self._lock:
            task = self._tstore.find_task(task_id, task_type)
            if task and task.get_worker() == worker_id:
                # Handle case when reducer fails to download the intermediate
                # files from worker which run mapper task
                if task_type == REDUCER and result == FILE_DOWNLOAD_FAILED:
                    # Check if all mappers are done, this avoid multiple
                    # reduce failures to reset the tasks
                    if self._mapper_done_evt.is_set():
                        # Clear mapper done event, so that mappers can be run again
                        self._mapper_done_evt.clear()
                        # Set mappers done event, so that processor stop the reducer execution
                        self._reducer_done_evt.set()
                        # Stop processor
                        self._task_processor.stop()
                        # Reset all tasks
                        self._tstore.reset_all_tasks()
                        # Delete all intermediate files infor from reducer tasks
                        self._tstore.delete_reducers_intermediate_files()
                        # Reset the mapper and reducer count
                        self._task_counter.mappers = self._tstore.get_no_of_mappers()
                        self._task_counter.reducers = self._tstore.get_no_of_reducers()
                        # Clear the reducer event so that reducers can be run again
                        self._reducer_done_evt.clear()
                        # Start processor
                        self._start_task_processor()
                    # Mark worker free
                    self._wregistry.mark_free(worker_id)
                    return
                elif result != TASK_FAILURE:
                    # Mark task processed and worker free
                    task.mark_processed()
                    task.set_finish_time()
                    self._wregistry.mark_free(worker_id)
                    # Add mapper produced intermediate files to worker info
                    if task_type == MAPPER:
                        for reducer_filename_tpl in result:
                            reducer_id = reducer_filename_tpl[0]
                            filename = reducer_filename_tpl[1]
                            reducer = self._tstore.find_task(str(reducer_id), REDUCER)
                            worker = self._wregistry.get(worker_id)
                            reducer.add_all_intermediate_files(
                                task_id, (worker.address, filename)
                            )
                    # Reduce marker and worker count
                    if task_type == MAPPER and self._task_counter.mappers > 0:
                        self._task_counter.mappers -= 1
                    elif task_type == REDUCER and self._task_counter.reducers > 0:
                        self._task_counter.reducers -= 1
                elif result == TASK_FAILURE:
                    task.reset()
                    self._wregistry.mark_free(worker_id)
                else:
                    LOG.debug("Recieved unknown response {}".format(result))
                    task.reset()
                    self._wregistry.mark_free(worker_id)
            else:
                # Mark worker free, there is a case where we reset all the tasks and
                # thats why worker ID does not match or task reply from former worker
                # is recieved later.
                self._wregistry.mark_free(worker_id)
