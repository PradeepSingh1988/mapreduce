"""" RPyc client for master and workers to make RPC calls"""

import copy
from functools import lru_cache
import logging

import rpyc

from mapr2.common.consts import HB_NOT_OK
from mapr2.core.response import HeartbeatResponse


LOG = logging.getLogger(__name__)


class ConnectionFailedException(Exception):
    pass


class FileReadFailedException(Exception):
    pass


class RPCClient(object):
    """
    Top level object to access RPyc API
    """

    def __init__(
        self,
        host="127.0.0.1",
        port=12345,
        keepalive=True,
    ):
        @lru_cache(maxsize=1)
        def c():
            try:
                rpc_client = rpyc.connect(host, port, keepalive=keepalive)
                return rpc_client
            except Exception as ex:
                LOG.debug("Connection failed to {}:{} due to {}".format(host, port, ex))
                raise ConnectionFailedException(
                    "Connection failed to {}:{} due to {}".format(host, port, ex)
                )

        self._conn = c

    def __del__(self):
        if self._conn.cache_info().hits > 0:
            self._conn().close()


class WorkerRPCClient(RPCClient):
    def get_heartbeat(self, message):
        try:
            result = copy.deepcopy(self._conn().root.worker.ping(message))
            return result
        except Exception as ex:
            # LOG.exception("Ping failed")
            return HeartbeatResponse(HB_NOT_OK)

    def send_mapper(self, task_id, task_file):
        try:
            result = copy.deepcopy(
                self._conn().root.worker.process_mapper(task_id, task_file)
            )
            return result
        except Exception as ex:
            LOG.exception("Could not submite mapper task")
            raise

    def send_reducer(self, task_id, copy_servers):
        try:
            result = copy.deepcopy(
                self._conn().root.worker.process_reducer(task_id, copy_servers)
            )
            return result
        except Exception as ex:
            LOG.exception("Could not submite reducer task")
            raise

    def read_file(self, filename):
        try:
            result = copy.deepcopy(self._conn().root.file.read(filename))
            return result
        except Exception as ex:
            LOG.exception("Could not read file {}".format(filename))
            raise FileReadFailedException("Could not read file {}".format(filename))


class MasterRPCClient(RPCClient):
    def register(self, worker_id, address):
        result = copy.deepcopy(self._conn().root.master.register(worker_id, address))
        return result

    def update_task_done(self, task_id, task_type, worker_id, result):
        try:
            result = copy.deepcopy(
                self._conn().root.master.update_task_done(
                    task_id, task_type, worker_id, result
                )
            )
            return result
        except Exception as ex:
            LOG.exception(
                "Could not update task status, {}:{}".format(task_id, task_type)
            )
            raise
