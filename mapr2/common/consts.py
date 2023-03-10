""" 
Module to define all constants
"""

HEARTBEAT_STATUS = (HB_OK, HB_NOT_OK) = ("OK", "NOT_OK")

TASK_STATES = (SCHEDULED, PROCESSING, PROCESSED, CANCELLED, IDLE) = (
    "Scheduled",
    "Processing",
    "Processed",
    "Cancelled",
    "Idle",
)

TASK_TYPES = (MAPPER, REDUCER) = ("Mapper", "Reducer")

NULL_WORKER_ID = None

REDUCER_COUNT = 10

TASK_FINISH_TIMEOUT = 120 * 1000  # in ms

FILE_DOWNLOAD_FAILED = 1

NODE_HEARTBEAT_INTERVAL = 15

TASK_FAILURE = "FAILED"
