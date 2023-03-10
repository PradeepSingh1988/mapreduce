import hashlib
import json
import logging
import os
import uuid

from mapr2.common.consts import MAPPER


LOG = logging.getLogger(__name__)


class Mapper:
    """
    Runs mapper function provided by the user.
    """

    task_type = MAPPER

    def __init__(
        self, mapper_fn, task_file_path, task_id, reducer_count, worker_id, tmp_dir
    ):
        self._mapper_function = mapper_fn
        self._file_path = task_file_path
        self._task_id = task_id
        self._reducer_count = reducer_count
        self._worker_id = worker_id
        self._tmp_dir = tmp_dir

    def _log(self, message):
        LOG.debug("W:{} M:{} Msg:{}".format(self._worker_id, self._task_id, message))

    def _rename_file(self, file_name):
        # Remove PID part from file
        base_name = os.path.basename(file_name).rsplit("-", 1)[0]
        directory_name = os.path.dirname(file_name)
        target_file = os.path.join(directory_name, base_name)
        os.rename(file_name, target_file)
        return target_file

    def _read_task_input_file(self):
        self._log("Reading input file {}".format(self._file_path))
        with open(self._file_path) as fd:
            return fd.read()

    def _ihash(self, key):
        return int(hashlib.sha512(key).hexdigest(), 16) % self._reducer_count

    def _write_map_stage_output(self, mapper_output):
        reducer_target_files = []
        opened_file_fds = []
        file_name_prefix = "{}/mr-{}".format(self._tmp_dir, self._task_id)
        for i in range(self._reducer_count):
            file_name = "{}-{}-{}".format(
                file_name_prefix, str(i), str(self._worker_id) + str(uuid.uuid4())[:4]
            )
            reducer_target_files.append(file_name)
            opened_file_fds.append(open(file_name, mode="a", buffering=1))
        for key_vlaue_pair in mapper_output:
            key = list(key_vlaue_pair.keys())[0].encode("utf-8")
            target_file_id = self._ihash(key)
            fd = opened_file_fds[target_file_id]
            fd.write("{}\n".format(json.dumps(key_vlaue_pair)))

        for fd in opened_file_fds:
            fd.close()
        result = []
        for index, file_name in enumerate(reducer_target_files):
            # renamed_file = self._rename_file(file_name)
            result.append((index, file_name))
        return result

    def run(self):
        content = self._read_task_input_file()
        self._log("Running Mapper function")
        mapper_output = self._mapper_function(self._file_path, content)
        self._log("Writing Result to file")
        return self._write_map_stage_output(mapper_output)
