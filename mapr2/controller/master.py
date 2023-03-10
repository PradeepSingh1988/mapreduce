"""
This module expose Master's API over RPC using RPyc module.
It runs a threaded server to accept one request per thread.
"""

import argparse
import glob
import logging
from threading import Thread
import time

import rpyc
from rpyc.utils.server import ThreadPoolServer

from mapr2.common.consts import REDUCER_COUNT
from mapr2.controller import exposify
from mapr2.core.master import Master


LOG = logging.getLogger(__name__)


@exposify
class exposed_Master(Master):
    pass


class MasterExecutorService(rpyc.Service):

    RPYC_PROTOCOL_CONFIG = rpyc.core.protocol.DEFAULT_CONFIG

    def __init__(self, master_obj):
        super().__init__()
        self.exposed_master = master_obj


def main():
    parser = argparse.ArgumentParser(
        prog="MasterExecutor",
        description="Execute the MapR jobs",
    )
    parser.add_argument(
        "--input-files", required=True, help="Can be full path or a wildcard pattern"
    )
    parser.add_argument(
        "--reducer-count", default=REDUCER_COUNT, help="Number of Reducers"
    )
    parser.add_argument(
        "--port", default=12345, help="Port Number"
    )

    args = parser.parse_args()
    input_files = glob.glob(args.input_files)
    if not input_files:
        print("No input files found")
        return
    master_obj = exposed_Master(input_files, int(args.reducer_count))
    master_executor_svc = MasterExecutorService(master_obj)

    server_logger = logging.getLogger("mcontroller")
    server_logger.setLevel(logging.WARNING)
    port = int(args.port)
    server = ThreadPoolServer(master_executor_svc, port=port, logger=server_logger)

    master_executor_thread = Thread(target=server.start)
    master_executor_thread.start()
    while not master_obj._is_done():
        time.sleep(0.5)

    time.sleep(5)
    server.close()


if __name__ == "__main__":
    main()
