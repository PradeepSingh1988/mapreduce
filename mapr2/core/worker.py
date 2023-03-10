import copy
import logging
import os
from queue import Empty, Queue
from threading import Event, Thread
import time
import traceback

from mapr2.common.client import (
    FileReadFailedException,
    MasterRPCClient,
)
from mapr2.common.consts import FILE_DOWNLOAD_FAILED, HB_OK, TASK_FAILURE
from mapr2.core.mapper import Mapper
from mapr2.core.reducer import Reducer
from mapr2.core.response import HeartbeatResponse

LOG = logging.getLogger(__name__)


class TaskRunner(Thread):
    """
    Thread which runs mapeer or reducer job o worker
    and update its run status to master
    """

    def __init__(self, task_queue, master_client):
        super().__init__()
        self._tqueue = task_queue
        self._stop = Event()
        self._rpc_master_client = master_client
        self.daemon = True

    def _update_status(self, task_id, task_type, worker_id, result):
        # If worker fails to update task status to master multiple times,
        # Master will consider this scenario as task timeout and rerun task again
        def _send():
            self._rpc_master_client.update_task_done(
                task_id, task_type, worker_id, result
            )

        failure_count = 0
        retry_count = 3
        sleep_interval = 2
        success = False
        while failure_count < retry_count:
            failure_count += 1
            try:
                _send()
                LOG.debug(
                    "Worker {} Sent status to master successfully for task {}:{}".format(
                        worker_id,
                        task_id,
                        task_type,
                    )
                )
                success = True
                break
            except Exception as ex:
                LOG.debug(
                    "Worker {} failed to Sent status to master for task {}:{}".format(
                        worker_id,
                        task_id,
                        task_type,
                    )
                )
                LOG.exception("Failed to update status")
                time.sleep(sleep_interval)
        if not success:
            LOG.debug(
                "Worker {} failed to Sent status to master for task {}:{}".format(
                    worker_id,
                    task_id,
                    task_type,
                )
            )

    def run(self):
        while not self._stop.wait(2):
            try:
                task = self._tqueue.get(block=False)
                LOG.debug("Starting to run opeartion")
                result = task.run()
                LOG.debug("result of Operation {}".format(result))
                self._update_status(
                    task._task_id, task.task_type, task._worker_id, result
                )
            except Empty:
                pass
            except FileReadFailedException:
                result = FILE_DOWNLOAD_FAILED
                self._update_status(
                    task._task_id, task.task_type, task._worker_id, FILE_DOWNLOAD_FAILED
                )
            except Exception as ex:
                LOG.exception("Operation Failed")
                result = TASK_FAILURE
                self._update_status(
                    task._task_id, task.task_type, task._worker_id, TASK_FAILURE
                )

    def stop(self):
        self._stop.set()


class Worker(object):
    """
    Worker Service which exposes its APIs to master and other workers.
    """

    def __init__(
        self,
        map_fn,
        reduce_fn,
        address,
        master_host="127.0.0.1",
        master_port=12345,
        tmp_dir="/tmp",
    ):
        self._address = address
        self._map_fn = map_fn
        self._reduce_fn = reduce_fn
        self._reducer_count = None
        self._tmp_dir = tmp_dir
        self._rpc_master_client = MasterRPCClient(master_host, master_port)
        self._worker_id = "w{}".format(os.getpid())
        self._task_queue = Queue()
        self._tprocessor = TaskRunner(self._task_queue, self._rpc_master_client)
        self._tprocessor.start()
        self._register()
        self._exit = False

    def _register(self):
        LOG.debug(
            "Registering with master {}:{}".format(self._address, self._worker_id)
        )
        self._reducer_count = self._rpc_master_client.register(
            self._worker_id, self._address
        )

    def ping(self, message):
        if message == "exit":
            self._exit = True
            self._tprocessor.stop()
        return HeartbeatResponse(status=HB_OK)

    def _is_done(self):
        return self._exit

    def process_mapper(self, task_id, task_file):
        LOG.debug("Recieved mapper task {}:{}".format(task_id, task_file))
        mapper = Mapper(
            self._map_fn,
            task_file,
            task_id,
            self._reducer_count,
            self._worker_id,
            self._tmp_dir,
        )
        self._task_queue.put(mapper)
        LOG.debug("Scheduled Mapper Task in queue {}:{}".format(task_id, task_file))

    def process_reducer(self, task_id, all_intermediate_files):
        LOG.debug(
            "Recieved Reducer task {} with files {}".format(
                task_id, all_intermediate_files
            )
        )
        reducer = Reducer(
            self._reduce_fn,
            self._tmp_dir,
            task_id,
            self._worker_id,
            copy.deepcopy(all_intermediate_files),
        )
        self._task_queue.put(reducer)
        LOG.debug("Scheduled Reducer {} Task in queue ".format(task_id))
