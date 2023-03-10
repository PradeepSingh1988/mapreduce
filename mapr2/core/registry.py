import json
import time

from threading import RLock


class Worker(object):
    """
    Worker object
    """

    def __init__(self, worker_id, address, is_healthy=True):
        self.worker_id = worker_id
        self.address = address
        self.is_healthy = is_healthy
        self._is_free = True
        self._last_heartbeat = time.time()

    def __repr__(self):
        return json.dumps(self.to_dict())

    @property
    def is_free(self):
        return self._is_free

    def to_dict(self):
        return {
            "worker_id": self.worker_id,
            "address": self.address,
            "healthy": self.is_healthy,
        }

    def __repr__(self):
        return json.dumps(self.to_dict())


class WorkerRegistry(object):
    """
    Worker Registry which stores all workers and
    provides APIs to get and set the info.
    """

    def __init__(self):
        self.workers = {}
        self._lock = RLock()

    def register(self, worker_id, address):
        with self._lock:
            if worker_id not in self.workers:
                self.workers[worker_id] = Worker(worker_id, address)
                return self.workers[worker_id]
            else:
                worker = self.workers[worker_id]
                if worker.address == address:
                    worker.mark_healthy()
                    worker.update_heartbeat()
                    return self.workers[worker_id]
            # Raise exception here?

    def unregister(self, worker_id):
        with self._lock:
            if worker_id in self.workers:
                del self.workers[worker_id]

    def list(self):
        with self._lock:
            return list(self.workers.values())

    def get(self, worker_id):
        with self._lock:
            if worker_id in self.workers:
                return self.workers[worker_id]

    def mark_healthy(self, worker_id):
        with self._lock:
            if worker_id in self.workers:
                self.workers[worker_id].is_healthy = True

    def mark_unhealthy(self, worker_id):
        with self._lock:
            if worker_id in self.workers:
                self.workers[worker_id].is_healthy = False

    def update_heartbeat(self, worker_id):
        with self._lock:
            if worker_id in self.workers:
                self.workers[worker_id]._last_heartbeat = time.time()

    def get_idle_server(self):
        with self._lock:
            for worker in self.workers.values():
                if worker.is_free and worker.is_healthy:
                    return worker

    def mark_busy(self, worker_id):
        with self._lock:
            if worker_id in self.workers:
                self.workers[worker_id]._is_free = False

    def mark_free(self, worker_id):
        with self._lock:
            if worker_id in self.workers:
                self.workers[worker_id]._is_free = True
