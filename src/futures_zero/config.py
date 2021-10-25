REQUEST_TIMEOUT = 2500

#  Paranoid Pirate Protocol constants
WORKER_READY_SIGNAL = b"\x01"  # Signals worker is ready
KILL_SIGNAL = b"\x03"

# Task modes signals
NORMAL_TASK_REQUEST_SIGNAL = b"\x04"
PANDAS_PARTITION_TASK_REQUEST_SIGNAL = b"\x05"
PANDAS_NONPARTITION_TASK_REQUEST_SIGNAL = b"\x06"

# Process start method signals
FORKED_PROCESS_SIGNAL = b"\x07"
SPAWNED_PROCESS_SIGNAL = b"\x08"

# Statefulness method signals
STATEFUL_METHOD_SIGNAL = b"\x10"  # b"\x9" is reserved
STATELESS_METHOD_SIGNAL = b"\x11"

# Task result signals
TASK_SUCCESS_SIGNAL = b"\x12"
TASK_FAILURE_SIGNAL = b"\x13"
WORKER_FAILURE_SIGNAL = b"\x14"
NUMPY_TASK_SUCCESS_SIGNAL = b"\x15"

# Endpoint address
SERVER_ENDPOINT = "tcp://127.0.0.1:5555"
WORKER_ENDPOINT = "tcp://127.0.0.1:5556"

DUMMY_TASK_KEY = b"\x16"

TMP_FILENAME = "MY_tmp_"
