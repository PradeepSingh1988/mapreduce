from collections import defaultdict
import glob
import json
import logging
import os
import uuid

from mapr2.common.client import WorkerRPCClient
from mapr2.common.consts import REDUCER

LOG = logging.getLogger(__name__)


class Reducer:
    """
    Reducer class that runs user provided reducer function.
    """

    task_type = REDUCER

    def __init__(self, reduce_fn, tmp_dir, task_id, worker_id, all_intermediate_files):
        self._reduce_fn = reduce_fn
        self._tmp_dir = tmp_dir
        self._task_id = task_id
        self._worker_id = worker_id
        self._all_intermediate_files = all_intermediate_files
        self._stop = False

    def _log(self, message):
        LOG.debug("W:{} R:{} Msg:{}".format(self._worker_id, self._task_id, message))

    def stop(self):
        self._log("Stopping reducer task")
        self._stop = True

    def _rename_file(self, file_name):
        # Remove Worker ID from File name
        base_name = os.path.basename(file_name).rsplit("-", 1)[0]
        directory_name = os.path.dirname(file_name)
        os.rename(file_name, os.path.join(directory_name, base_name))

    def _write_reduce_stage_output(self, key_value_list):
        op_file_name = "{}/mr-out-{}-{}".format(
            self._tmp_dir,
            self._task_id,
            self._worker_id + str(uuid.uuid4())[:4],
        )
        sorted_keys = sorted(key_value_list.keys())
        with open(op_file_name, "a") as fd:
            for key in sorted_keys:
                if self._stop:
                    break
                values = key_value_list[key]
                result = self._reduce_fn(key, values)
                fd.write("{} {}\n".format(key, result))
        if not self._stop:
            self._rename_file(op_file_name)

    def _get_file_from_remote_worker(self, file_details):
        host, port = file_details[0][0], file_details[0][1]
        file_name = file_details[1]
        self._log("Reading file {} from {}:{}".format(file_name, host, port))
        client = WorkerRPCClient(host, port)
        content = client.read_file(file_name)
        return content

    def _cleanup(self):
        # Remove temporary files
        try:
            file_names = glob.glob(
                "{}/mr-out-{}-{}{}".format(
                    self._tmp_dir, self._task_id, self._worker_id, "*"
                )
            )
            for file_name in file_names:
                os.remove(file_name)
        except Exception as ex:
            self._log("File delete afiled due to {}".format(ex))
            LOG.exception("File cleanup failed")

    def run(self):
        key_value_list = defaultdict(list)
        for file_details in self._all_intermediate_files:
            if self._stop:
                break
            content = self._get_file_from_remote_worker(file_details)
            for key_val in content.strip().split("\n"):
                try:
                    unmarshelled_key_val = json.loads(key_val)
                    for key, value in unmarshelled_key_val.items():
                        key_value_list[key].append(value)
                except Exception as ex:
                    self._log("Json decoding failed for key/val '{}'".format(key_val))
            self._write_reduce_stage_output(key_value_list)
        if self._stop:
            self._cleanup()
