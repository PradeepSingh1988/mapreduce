import json
import time
import logging

from threading import Lock

from mapr2.common.consts import (
    IDLE,
    MAPPER,
    PROCESSED,
    PROCESSING,
    REDUCER,
    SCHEDULED,
    NULL_WORKER_ID,
)


LOG = logging.getLogger(__name__)


class Task:
    """Task Object"""

    def __init__(
        self,
        task_id,
        task_type,
        task_state=IDLE,
        worker_id=NULL_WORKER_ID,
    ):
        self._task_id = task_id
        self._task_type = task_type
        self._task_state = task_state
        self._worker = worker_id
        self._start_time = None
        self._finish_time = None

    def get_id(self):
        return self._task_id

    def get_type(self):
        return self._task_type

    def get_start_time(self):
        return self._start_time

    def get_finish_time(self):
        return self._finish_time

    def is_mapper(self):
        return self._task_type == MAPPER

    def is_reducer(self):
        return self._task_type == REDUCER

    def is_idle(self):
        return self._task_state == IDLE

    def is_running(self):
        return self._task_state == PROCESSING

    def is_completed(self):
        return self._task_state == PROCESSED

    def mark_idle(self):
        self._task_state = IDLE

    def mark_processed(self):
        self._task_state = PROCESSED

    def mark_processing(self):
        self._task_state = PROCESSING

    def mark_scheduled(self):
        self._task_state = SCHEDULED

    def set_worker(self, worker_id):
        self._worker = worker_id

    def set_start_time(self):
        self._start_time = round(time.time() * 1000)

    def set_finish_time(self):
        self._finish_time = round(time.time() * 1000)

    def get_worker(self):
        return self._worker

    def get_task_state(self):
        return self._task_state

    def get_task_file(self):
        return None

    def reset(self):
        self.mark_idle()
        self.set_worker(NULL_WORKER_ID)
        self._start_time = None
        self._finish_time = None

    def __repr__(self):
        return json.dumps(
            {
                "task_type": self.get_type(),
                "task_id": self.get_id(),
                "task_state": self.get_task_state(),
                "worker_id": self.get_worker(),
                "start_time": self.get_start_time(),
                "finish_time": self.get_finish_time(),
            }
        )


class MapperTask(Task):
    """Mapper Task Object"""

    def __init__(self, task_id, task_file):
        super().__init__(task_id, MAPPER)
        self._task_file = task_file

    def get_task_file(self):
        return self._task_file


class ReducerTask(Task):
    """Reducer Task Object"""

    def __init__(self, task_id):
        super().__init__(task_id, REDUCER)
        self._intermediate_file_details = {}

    def add_all_intermediate_files(self, mapper_id, file_details):
        self._intermediate_file_details[mapper_id] = file_details

    def get_all_intermediate_files(self):
        return list(self._intermediate_file_details.values())

    def drop_dead_mappers_files(self, mapper_id=None):
        if mapper_id:
            if mapper_id in self._intermediate_file_details:
                del self._intermediate_file_details[mapper_id]
        else:
            self._intermediate_file_details = {}


class TaskStore:
    """This stores all the tasks and provides APIs for accessing tasks"""

    def __init__(self):
        self._mappers = []
        self._reducers = []
        self._lock = Lock()

    def add_mapper(self, mapper_task):
        with self._lock:
            self._mappers.append(mapper_task)

    def add_reducer(self, reducer_task):
        with self._lock:
            self._reducers.append(reducer_task)

    def _find_mapper(self, task_id):
        for mapper in self._mappers:
            if mapper.get_id() == task_id:
                return mapper
        return None

    def _find_reducer(self, task_id):
        for reducer in self._reducers:
            if reducer.get_id() == task_id:
                return reducer
        return None

    def find_task(self, task_id, task_type):
        with self._lock:
            if task_type == MAPPER:
                return self._find_mapper(task_id)
            else:
                return self._find_reducer(task_id)

    def find_idle_mapper(self):
        with self._lock:
            for idle_mapper in [mapper for mapper in self._mappers if mapper.is_idle()]:
                return idle_mapper

    def find_idle_reducer(self):
        with self._lock:
            for idle_reducer in [
                reducer for reducer in self._reducers if reducer.is_idle()
            ]:
                return idle_reducer

    def get_no_of_mappers(self):
        with self._lock:
            return len(self._mappers)

    def get_no_of_reducers(self):
        with self._lock:
            return len(self._reducers)

    def _list_all_finished_mappers(self, worker_id):
        return [
            task
            for task in self._mappers
            if task.is_completed() and task.get_worker() == worker_id
        ]

    def list_all_running_tasks(self):
        with self._lock:
            return [
                task for task in self._mappers + self._reducers if task.is_running()
            ]

    def _list_all_running_tasks_on_worker(self, worker_id):
        return [
            task
            for task in self._mappers + self._reducers
            if task.is_running() and task.get_worker() == worker_id
        ]

    def _reset_task(self, task):
        LOG.debug(
            "Marking {} task {}:{} Idle".format(
                task.get_task_state(), task.get_id(), task.get_type()
            )
        )
        task.reset()

    def list_all_running_tasks_on_worker(self, worker_id):
        with self._lock:
            return self._list_all_running_tasks_on_worker(worker_id)

    def reset_all_tasks(self):
        with self._lock:
            for task in self._mappers + self._reducers:
                self._reset_task(task)

    def reset_all_running_tasks_on_worker(self, worker_id):
        with self._lock:
            for task in self._list_all_running_tasks_on_worker(worker_id):
                self._reset_task(task)

    def _drop_reducers_file_info(self, mapper_id):
        for task in self._reducers:
            task.drop_dead_mappers_files(mapper_id)

    def delete_reducers_intermediate_files(self):
        with self._lock:
            for task in self._reducers:
                task.drop_dead_mappers_files()

    def reset_all_finished_mappers_on_worker(self, worker_id, del_reducer_file=True):
        with self._lock:
            reset_count = 0
            for task in self._list_all_finished_mappers(worker_id):
                self._reset_task(task)
                reset_count += 1
                if del_reducer_file:
                    self._drop_reducers_file_info(task.get_id())
            return reset_count
