import logging
from threading import Event, Thread
import time

from mapr2.common.client import WorkerRPCClient
from mapr2.common.consts import HB_NOT_OK, HB_OK, NODE_HEARTBEAT_INTERVAL


LOG = logging.getLogger(__name__)


class HeartbeatMonitor(Thread):
    """
    Sends periodic heartbeats to worker to check their aliveness.
    If they dont respnd then mark them as failed and put them in
    node failure queue.
    """

    def __init__(self, worker_registry, failed_node_queue):
        super().__init__()
        self._wregistry = worker_registry
        self._stop = Event()
        self._worker_exit = False
        self._failed_node_queue = failed_node_queue

    def _check_heartbeat(self, address, worker):
        prev_is_healthy = worker.is_healthy
        client = WorkerRPCClient(address[0], address[1])
        message = "exit" if self._worker_exit else "ping"
        heartbeat = client.get_heartbeat(message)
        LOG.debug(
            "Got HB message {} from {}:{}".format(
                heartbeat.status, worker.worker_id, address
            )
        )
        if heartbeat.status == HB_OK:
            self._wregistry.update_heartbeat(worker.worker_id)
        elif heartbeat.status == HB_NOT_OK and message == "ping":
            if prev_is_healthy:
                self._wregistry.mark_unhealthy(worker.worker_id)
                self._failed_node_queue.put(worker.worker_id)

        elif heartbeat.status == HB_NOT_OK and message == "exit":
            LOG.debug(
                "Worker {} did not respond, exiting anyway".format(worker.worker_id)
            )
        else:
            LOG.debug("Worker {} sent unknown message".format(worker.worker_id))
            pass

    def run(self):
        while True:
            if self._stop.is_set():
                self._worker_exit = True
            workers = self._wregistry.list()
            hb_threads = []
            for worker in workers:
                hb_threads.append(
                    Thread(target=self._check_heartbeat, args=(worker.address, worker))
                )
            for th in hb_threads:
                th.start()
            for th in hb_threads:
                th.join()
            if self._worker_exit:
                break
            time.sleep(NODE_HEARTBEAT_INTERVAL)

    def stop(self):
        LOG.debug("Heartbeat Monitor Recieved stop signal")
        self._stop.set()


class NodeFailureHandler(Thread):
    """
    Reads failed node ID from failure node queue and resets all
    running tasks on them, and if mappers are still running then
    it reset all processed mappers on the worker. Please note it does
    not reset processed mappers once mappers phase is finished. That
    flow is handled by master once reducer reports that it is not able to
    download intermediate mapper file from a files node. In such cases master
    reset all tasks and starts whole map reduce program again.
    """

    def __init__(
        self,
        task_store,
        failed_node_queue,
        mapper_finished_event,
        global_lock,
        task_counter,
    ):
        super().__init__()
        self._tstore = task_store
        self._failed_node_queue = failed_node_queue
        self._mapper_fin_evt = mapper_finished_event
        self._stop = Event()
        self._global_lock = global_lock
        self._task_counter = task_counter
        self.daemon = True

    def run(self):
        while not self._stop.is_set():
            failed_worker_id = self._failed_node_queue.get()
            LOG.debug(
                "Resetting all running tasks on unhealthy worker {}".format(
                    failed_worker_id
                )
            )
            with self._global_lock:
                self._tstore.reset_all_running_tasks_on_worker(failed_worker_id)
                if not self._mapper_fin_evt.is_set():
                    LOG.debug(
                        "Resetting all finished mappers on unhealthy worker {}".format(
                            failed_worker_id
                        )
                    )
                    reset_count = self._tstore.reset_all_finished_mappers_on_worker(
                        failed_worker_id
                    )
                    if reset_count:
                        self._task_counter.mappers += reset_count

    def stop(self):
        LOG.debug("Node Failure Handler Recieved stop signal")
        self._stop.set()
