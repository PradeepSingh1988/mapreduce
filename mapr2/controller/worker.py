"""
This module expose Worker's API over RPC using RPyc module.
It runs a threaded server to accept one request per thread.
"""

import argparse
import logging
import pickle
from threading import Thread
import time

import rpyc
from rpyc.utils.server import ThreadPoolServer

from mapr2.controller import exposify
from mapr2.core.file_transfer import File
from mapr2.core.worker import Worker


LOG = logging.getLogger(__name__)


def _load_functions(pickled_file_path):
    with open(pickled_file_path, "rb") as fd:
        un_pickled_map = pickle.load(fd)
        map_fn = un_pickled_map["mapper"]
        reduce_fn = un_pickled_map["reducer"]
        return map_fn, reduce_fn


@exposify
class exposed_Worker(Worker):
    pass


@exposify
class exposed_File(File):
    pass


class WorkerExecutorService(rpyc.Service):

    RPYC_PROTOCOL_CONFIG = rpyc.core.protocol.DEFAULT_CONFIG

    def __init__(self, worker_obj):
        super().__init__()
        self.exposed_worker = worker_obj
        self.exposed_file = exposed_File()


def main():
    parser = argparse.ArgumentParser(
        prog="Worker",
        description="Execute the MapR jobs",
    )
    parser.add_argument("--master-host", required=True, help="Master IP")

    parser.add_argument("--master-port", required=True, help="Master PORT")
    parser.add_argument("--worker-port", required=True, help="Worker PORT")
    parser.add_argument("--tmp-dir", help="Directory where Files will be written")

    parser.add_argument(
        "--pickle-file-path", help="Path of the pickled map reduce job file"
    )
    args = parser.parse_args()

    map_fn, reduce_fn = _load_functions(args.pickle_file_path)
    worker_port = int(args.worker_port)
    worker_obj = exposed_Worker(
        map_fn,
        reduce_fn,
        ("127.0.0.1", worker_port),
        args.master_host,
        args.master_port,
        args.tmp_dir,
    )
    worker_executor_svc = WorkerExecutorService(worker_obj)

    server_logger = logging.getLogger("wcontroller")
    server_logger.setLevel(logging.WARNING)
    server = ThreadPoolServer(
        worker_executor_svc, port=worker_port, logger=server_logger
    )

    worker_executor_thread = Thread(target=server.start)
    worker_executor_thread.start()
    while not worker_obj._is_done():
        time.sleep(5)

    time.sleep(5)
    server.close()


if __name__ == "__main__":
    main()
