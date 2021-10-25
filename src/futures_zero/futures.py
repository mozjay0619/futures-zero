import itertools
import logging
import os
import sys
import tempfile
import time
import warnings
from collections import defaultdict
from multiprocessing import set_start_method
from random import randint

import cloudpickle
import feather
import msgpack
import numpy as np
import pandas as pd
import psutil
import zmq

from .config import *
from .server import ServerProcess
from .utils import ProgressBar
from .worker import WorkerProcess


class WORKER_FAILED(UserWarning):
    pass


class Futures:
    """Parallelization using ZeroMQ message passing framework. The architecture
    design is based on the Paranoid Pirate Pattern described in
    "https://zguide.zeromq.org/docs/chapter4/".

    This class is responsible for starting the daemon process for the proxy server
    and the daemon processes for the workers. The client runs on the main process.
    When the client submits tasks, they are queued in the server proxy server socket.
    The server will load balance the tasks to the LRU (least recently used) worker.
    Once the worker finishes the task, it will send the results message to the server,
    which will in turn send the message back to the client in the main process.


                                                                      worker (client)
    client <--asynch msg queue--> proxy server <--asynch msg queue--> worker (client)
                                                                      worker (client)


    There are 5 different ways to submit the user functions:

    - submit
    - submit_keyed
    - submit_stateful
    - apply
    - capply

    Refer to the method docstrings below and the accompanying jupyter notebook
    tutorial for their use cases.

    Parameters
    ----------
    n_workers : int
        The number of worker processes to use. Default is the number of available
        physical cores.

    start_method : str, Default
        The process start method. Available options: "fork", "spawn", and "forkserver".
        Default is "fork".

    verbose : int
        The levels of verboseness. Available options: 0, 1, 2. Default is 0.

    worker : WorkerProcess
        The subclass of WorkerProcess for custom initialization of the worker process.

    worker_args : list
        The positional arguments for the WorkerProcess.

    worker_kwargs : dict
        The keyword arguments for the WorkerProcess.

    n_retries : int
        The number of retries for any given task.
    """

    def __init__(
        self,
        n_workers=None,
        start_method="fork",
        verbose=0,
        n_retries=2,
        worker=None,
        *worker_args,
        **worker_kwargs,
    ):

        self.n_workers = n_workers
        self.start_method = start_method
        self.verbose = verbose
        self.request_retries = n_retries
        self.worker = worker
        self.worker_args = worker_args
        self.worker_kwargs = worker_kwargs

        self.worker_procs = []

        if n_workers is None:

            self.n_workers = psutil.cpu_count(logical=False)

        set_start_method(self.start_method, force=True)

        self.dataframe = None
        self.results = dict()
        self.errors = dict()
        self._task_keys = set()
        self._pending_tasks = dict()
        self._failed_tasks = dict()
        self._fail_counter = defaultdict(int)

        self.server_client_online = False
        self.mode = "normal"
        self.sub_mode = defaultdict(bool)
        self.sub_mode_secondary_checker = dict()
        self.temp_dir = None

    def print(self, s, lvl):

        if self.verbose >= lvl:
            print(s)

    def start_workers(self, n_workers):
        """Start the worker processes, either the default WorkerProcess class
        or overridden WorkerProcess subclass.

        Parameters
        ----------
        n_workers : int
            The number of worker processes to use.
        """
        for _ in range(n_workers):

            if self.worker is not None:

                worker_proc = self.worker(
                    __verbose__=self.verbose,
                    __dataframe__=self.dataframe,
                    __forked__=self.start_method == "fork",
                    __mode__=self.mode,
                    __partition__=self.sub_mode["partition"],
                    *self.worker_args,
                    **self.worker_kwargs,
                )

            else:

                # If forked, passing ``self.dataframe`` won't create a copy.
                #
                worker_proc = WorkerProcess(
                    __verbose__=self.verbose,
                    __dataframe__=self.dataframe,
                    __forked__=self.start_method == "fork",
                    __mode__=self.mode,
                    __partition__=self.sub_mode["partition"],
                )

            worker_proc.daemon = True
            worker_proc.start()

            self.worker_procs.append(worker_proc)

    def start_server(self):
        """Start the server process."""
        self.server_process = ServerProcess(self.client_address, self.verbose)
        self.server_process.daemon = True
        self.server_process.start()

    def start_client(self):
        """Connect the client socket to the server endpoint. Assign the
        client the string address (the current process's PID).
        """
        self.client_address = str(os.getpid()).encode()
        self.context = zmq.Context()
        self.client = self.context.socket(zmq.DEALER)
        self.client.setsockopt(zmq.IDENTITY, self.client_address)
        self.client.connect(SERVER_ENDPOINT)

        self.print("CLIENT STARTED: {}\n\n".format(self.client_address), 1)

    def start(self):
        """Utility function to start the processes and bind the sockets.
        Also, the ``server_client_online`` flag is set to True, so that the
        Futures object knows not to restart the processes.
        """
        self.start_workers(self.n_workers)
        self.start_client()
        self.start_server()

        self.server_client_online = True

    def close(self):
        """Gracefully terminate all the network sockets and processes with
        timeout by sending them the kill signal. If there are remaining worker
        processes after the set timeout, those processes are forcefully
        terminated.
        """
        frames = [DUMMY_TASK_KEY, KILL_SIGNAL]

        # The DEALER socket will prepend the client address.
        # [dummy_task_key, kill_signal]
        self.client.send_multipart(frames)

        self.server_client_online = False

        close_start_time = time.time()

        while (
            any([proc.is_alive() for proc in self.worker_procs])
            and time.time() - close_start_time < 1
        ):

            time.sleep(0.01)

        for proc in self.worker_procs:

            if proc.is_alive():

                self.print("FORCEFULLY TERMINATING WORKER", 1)

                proc.terminate()

    def clear(self):
        """Clean out the computation results and data from memory."""
        self.dataframe = None
        self._task_keys = set()
        self._pending_tasks = dict()
        self.results = dict()
        self.errors = dict()
        self._failed_tasks = dict()
        self.sub_mode = defaultdict(bool)
        self.sub_mode_secondary_checker = dict()

    def apply_to(self, dataframe, groupby=None, orderby=None):
        """Prepare the dataframe for parallel processing. This procedure
        depends on the process start method.

        If the startmethod is forking, then we will rely on the Unix COW
        to bring the dataframe to the subprocesses. Otherwise, we will write
        the dataframe on disk using feather and read it from subprocesses.

        If groupby is not None, then we will partition the dataframe by the
        groupby column, and the dict will be saved in global namespace in
        case of forking, or partitioned dataframes on disk using feather
        otherwise (the feather writing of partitions is done in parallel using
        forking).

        Parameters
        ----------
        dataframe : Pandas dataframe

        groupby : str

        orderby : str

        """

        self.mode = "pandas"

        if groupby:

            self.sub_mode["partition"] = True

            dataframe_dict = dict()

            for key, sub_df in dataframe.groupby(groupby):

                if orderby:

                    sub_df = sub_df.sort_values(orderby)

                sub_df.reset_index(drop=True, inplace=True)
                sub_df = sub_df.copy(deep=False)

                dataframe_dict[key] = sub_df

            dataframe = dataframe_dict

        else:

            self.sub_mode["partition"] = False

            if orderby:

                dataframe = dataframe.sort_values(orderby)
                dataframe.reset_index(drop=True, inplace=True)
                dataframe = dataframe.copy(deep=False)

        if self.start_method == "fork":

            set_start_method(self.start_method, force=True)

            if groupby:

                self.dataframe = dataframe_dict

            else:

                self.dataframe = dataframe

        # If the processes are not forked, then write the dataframe on disk.
        # If groupby, then use forking to write the dataframe partitions in
        # parallel.
        else:

            assert self.start_method in ["spawn", "forkserver"]
            set_start_method(self.start_method, force=True)

            if self.sub_mode["partition"]:

                pass

            else:

                cur_dirpath = os.getcwd()
                temp_filepath = os.path.join(cur_dirpath, "__futures_zero_dataframe__")
                feather.write_dataframe(dataframe, temp_filepath)

                self.dataframe = temp_filepath

            # if groupby:

            #     global global_dataframe
            #     global_dataframe = dataframe

            #     def write_dataframe(key=None):

            #         global global_dataframe

            #         if isinstance(global_dataframe, dict):

            #             filename = "".join([TMP_FILENAME, str(key)])
            #             filepath = os.path.join(os.getcwd(), filename)

            #             feather.write_dataframe(global_dataframe[key], filepath)

            #         else:

            #             print(global_dataframe)

            #     for key in dataframe.keys():

            #         self.submit_keyed(key, write_dataframe, key)

    def apply(self, func, *args, __key__=None, **kwargs):
        """Apply the user function on the Pandas dataframe passed in the ``apply_to``
        method. The ``func`` method signature requires the dataframe as the first
        positional argument.

        Parameters
        ----------
        func : Python method

        __key__ : Python object
        """
        self.mode = "pandas"

        key = __key__ or len(self._task_keys)

        if isinstance(key, list):
            key = tuple(key)

        binary = self._serialize(key, False, func, *args, **kwargs)

        self._submit(key, binary)

    def capply(self, column=None, func=None, *args, **kwargs):
        """``capply`` stands for column-apply. Same as the ``apply`` method,
        but the ``func`` method has an additional requirement that the return
        value is the numpy array with the same length as the dataframe. That
        array will be appended to the input dataframe with the name ``column``.

        You can also give the ``column`` a list of string names if the return value
        of the ``func`` is a multidimensional array. Then the dataframe will get
        multiple columns, using column names from the ``column`` list.

        Parameters
        ----------
        func : Python method

        column : str or list or str

        args : Python objects
            Positional arguments

        """
        self.sub_mode["column"] = True

        key = column or len(self._task_keys)

        if isinstance(key, list):
            key = tuple(key)

        self.sub_mode_secondary_checker[key] = "column"

        self.apply(func, *args, __key__=key, **kwargs)

    def submit(self, func, *args, __key__=None, __stateful__=False, **kwargs):
        """Pass in user func to compute in parallel.

        Parameters
        ----------
        func : Python method

        __key__ : Python object

        __stateful__ : boolean
            If True, the ``func`` method must have "self" as the first positional
            argument, where that self is the WorkerProcess instance.
        """
        self.mode = "normal"

        key = __key__ or len(self._task_keys)

        if isinstance(key, list):
            key = tuple(key)

        binary = self._serialize(key, __stateful__, func, *args, **kwargs)

        self._submit(key, binary)

    def submit_keyed(self, key, func, *args, __stateful__=False, **kwargs):
        """Same as ``submit``, but key positional argument is required.

        Parameters
        ----------
        key : Python object

        func : Python method
        """
        self.submit(func, *args, __key__=key, __stateful__=__stateful__, **kwargs)

    def submit_stateful(self, func, *args, __key__=None, **kwargs):
        """Same as ``submit`` but the user method signature has a requirement
        that "self" is the first positional argument. This "self" is the
        WorkerProcess instance. This method is same as ``submit`` with
        __stateful__ set to True.

        Parameters
        ----------
        func : Python method

        __key__ : Python object
        """
        self.submit(func, *args, __key__=__key__, __stateful__=True, **kwargs)

    def submit_stateful_keyed(self, key, func, *args, **kwargs):
        """Utility function combining ``submit_keyed`` and ``submit_stateful``.

        Parameters
        ----------
        func : Python method

        key : Python object
        """
        self.submit(func, *args, __key__=key, __stateful__=True, **kwargs)

    def _serialize(self, key, stateful, func, *args, **kwargs):
        """The ``func`` method is serialized using cloudpickle. The arguments
        are serialized using msgpack. We will also attach 3 signals to the
        message: task mode (e.g. normal, pandas), process start method
        (e.g. fork), and statefulenss message.

        The return value is the message that will be passed over to the server,
        which will route the message in turn to the LRU workers. This message
        consists of a list of binary streams:

        0. key
        1. task mode
        2. process start method
        3. statefulness of method
        4. func
        5. args (binary of args and kwargs)
        """

        # Serialize the task payload, comprised of the user method and method arguments.
        binary_func = cloudpickle.dumps(func)
        binary_args = msgpack.packb([args, kwargs], use_bin_type=True)
        binary_key = msgpack.packb(key, use_bin_type=True)

        task_ctrl_msg = []

        # Task mode message.
        if self.mode == "normal":

            task_ctrl_msg.append(NORMAL_TASK_REQUEST_SIGNAL)  # b"\x04"

        elif self.mode == "pandas":

            if self.sub_mode["partition"]:

                task_ctrl_msg.append(PANDAS_PARTITION_TASK_REQUEST_SIGNAL)  # b"\x05"

            else:

                task_ctrl_msg.append(PANDAS_NONPARTITION_TASK_REQUEST_SIGNAL)  # b"\x06"

        # Process start method message.
        if self.start_method == "fork":

            task_ctrl_msg.append(FORKED_PROCESS_SIGNAL)  # b"\x07"

        elif self.start_method in ["spawn", "forkserver"]:

            task_ctrl_msg.append(SPAWNED_PROCESS_SIGNAL)  # b"\x08"

        else:

            raise ValueError("Not a valid start method.")

        # User method statefulness message.
        if stateful:

            task_ctrl_msg.append(STATEFUL_METHOD_SIGNAL)  # b"\x10"

        else:

            task_ctrl_msg.append(STATELESS_METHOD_SIGNAL)  # b"\x11"

        return [binary_key] + task_ctrl_msg + [binary_func, binary_args]

    def _submit(self, key, request):
        """Utility function to send work request to the server (which will in turn delegate
        the message to the first available LRU worker). Once the work is submitted, that
        request message will be added to pending tasks list and the key will be added to the
        task keys list. Importantly, if the server client are not online, we will call the
        utility start method to start the daemon processes.

        Parameters
        ----------
        key : binary

        request : list of binaries
                        0. key
                        1. task mode
                        2. process start method
                        3. statefulness of method
                        4. func
                        5. args (binary of args and kwargs)
        """
        if not self.server_client_online:
            self.start()
            time.sleep(0.05)

        self.print(f"1. SENDING FROM CLIENT TO SERVER: {request}\n", 2)
        self.print("** NOTE: b'\\x92\\x90\\x80' is [[], {}]\n\n", 2)
        self.print(
            "1. SENDING FROM CLIENT TO SERVER: [task_key, task_mode_signal, start_method_signal, "
            "func_statefulness_signal, func, args]",
            1,
        )

        # The DEALER socket will prepend the client address.
        # [task_key, task_mode_signal, start_method_signal, func_statefulness_signal, func, args]
        self.client.send_multipart(request)

        self._pending_tasks[key] = request
        self._task_keys.add(key)

    def _handle_failed_tasks(self, task_key, error_msg):
        """Failed task retry mechanism. If the task has failed less than the set threshold counts,
        re-submit the task. Otherwise, remove the task from pending tasks list and put it in the
        failed tasks list and record the error.
        """
        self._fail_counter[task_key] += 1

        if self._fail_counter[task_key] < self.request_retries + 1:

            self.print(
                f"9. TASK {task_key} FAILED {self._fail_counter[task_key]} TIME(S), RETRYING\n\n",
                1,
            )

            self._submit(task_key, self._pending_tasks.pop(task_key, None))

        else:

            self.print(
                f"9. TASK {task_key} FAILED {self._fail_counter[task_key]} TIME(S), ABORTING\n\n",
                1,
            )

            # Place the task in the failed tasks dictionary.
            self._failed_tasks[task_key] = self._pending_tasks.pop(task_key, None)

            self.errors[task_key] = error_msg

    def _poll(self):
        """Start the infinite while loop to listen to the server for replies. Continue the
        loop while the sum of results, errors and dead workers is less than the total number
        of submitted tasks.

        There are two types of successful signals:
            - normal: the payload is deserialized using msgpack.
            - numpy: the numpy array is reconstructed using memoryview from the memory buffer

        There are two types of failure signals:
            - task failure: error caused by the user ``func``.
            - worker failure: error caused by death of the process itself.

        The server cannot see the worker death and therefore the client from main process
        will check if there are workers that are dead. If there are, it will start as many
        processes as they died. The tasks that were never completed by those processes will
        be re-submited.
        """
        while len(self.results) + len(self.errors) < len(self._task_keys):

            # Start listening to replies from the server
            if (self.client.poll(REQUEST_TIMEOUT) & zmq.POLLIN) != 0:

                # The REQ socket stripped off the client address.
                # [task_key, task_signal, error_msg, task_signal, task_signal, func, args] or
                # [task_key, task_signal, result] or
                # [dummy_task_key, worker_failure_signal, failed_task_keys]
                reply = self.client.recv_multipart()

                task_key = msgpack.unpackb(reply[0], raw=False)  # task_key

                if isinstance(task_key, list):
                    task_key = tuple(task_key)

                reply_payload = reply[1:]
                # [task_success_signal, result]
                # [numpy_task_success_signal, metadata, result]
                # [task_signal, error_msg]
                # [worker_failure_signal, failed_task_keys]

                # If the task is returning general python object(s), use msgpack to deserialize data.
                if reply_payload[0] == TASK_SUCCESS_SIGNAL:

                    self.print(f"8. RECEIVED FROM SERVER IN CLIENT: {reply}\n", 2)
                    self.print(
                        "8. RECEIVED FROM SERVER IN CLIENT: [task_key, task_success_signal, result]\n\n",
                        1,
                    )

                    # Recover the returned data.
                    result = msgpack.unpackb(reply_payload[1], raw=False)

                    self.bar.report(len(self.results))

                    # Remove the task from pending status only after checking the results.
                    self._pending_tasks.pop(task_key, None)

                    self.results[task_key] = result

                # If the task returns a single numpy array, use memoryview and memory buffer to recover the data.
                elif reply_payload[0] == NUMPY_TASK_SUCCESS_SIGNAL:

                    self.print(f"8. RECEIVED IN CLIENT FROM SERVER: {reply}\n", 2)
                    self.print(
                        "8. RECEIVED IN CLIENT FROM SERVER: [task_key, numpy_task_success_signal, metadata, result]\n\n",
                        1,
                    )

                    # Recover the numpy array from the binary stream.
                    metadata = msgpack.unpackb(reply_payload[1], raw=False)
                    buf = memoryview(reply_payload[2])
                    result = np.frombuffer(buf, dtype=metadata["dtype"]).reshape(
                        metadata["shape"]
                    )

                    if self.mode == "pandas":

                        # If ``capply`` was called, append the array as a column to the input dataframe.
                        if (
                            self.sub_mode["column"]
                            and self.sub_mode_secondary_checker.get(task_key)
                            == "column"
                        ):

                            # If groupby was not used
                            if not self.sub_mode["partition"]:

                                self.print(
                                    "9. MODE: PANDAS, SUBMODES: COLUMN\n\n",
                                    1,
                                )

                                if (
                                    (isinstance(result, np.ndarray))
                                    & (len(result) == len(self.dataframe))
                                    & (not isinstance(task_key, int))
                                ):

                                    # If the ``column`` was made of multiple column names.
                                    if isinstance(task_key, tuple):

                                        # Only if the number of returned columns equals the suggested
                                        # number of columns.
                                        if result.shape[1] == len(task_key):

                                            res_df = pd.DataFrame(
                                                result, columns=task_key
                                            )
                                            self.dataframe = self.dataframe.join(res_df)
                                            self.results[task_key] = None

                                        else:

                                            warning_msg = (
                                                "The resulting column could not be attached to "
                                                "the dataframe because result has different number of columns. "
                                                "Recording the results in 'results' dict"
                                            )
                                            warnings.warn(warning_msg)

                                            self.results[task_key] = result

                                    # If the ``column`` was a single column name.
                                    else:

                                        # Only if the returned array is 1-dimensional.
                                        if len(result.shape) == 1:

                                            self.dataframe[task_key] = result
                                            self.results[task_key] = None

                                        else:

                                            warning_msg = (
                                                "The resulting column could not be attached to "
                                                "the dataframe because result has too many columns. "
                                                "Recording the results in 'results' dict"
                                            )
                                            warnings.warn(warning_msg)

                                            self.results[task_key] = result

                                # Either the result is not ndarray or
                                # the length of the returned array is not the same as the dataframe's or
                                # task_key is an integer value.
                                else:

                                    print(result, "+++")
                                    print(task_key, "+++")

                                    warning_msg = (
                                        "The resulting column could not be attached to "
                                        "the dataframe. Recording the results in 'results' dict"
                                    )
                                    warnings.warn(warning_msg)

                                    self.results[task_key] = result

                            # If groupby was used
                            else:

                                self.print(
                                    "9. MODE: PANDAS, SUBMODES: COLUMN, PARTITION\n\n",
                                    1,
                                )

                                raise NotImplementedError(
                                    "Parition submode is not yet supported"
                                )

                        # If only ``apply`` was called and not ``capply``.
                        else:

                            self.print(
                                "9. MODE: PANDAS, SUBMODES:\n\n",
                                1,
                            )

                            self.results[task_key] = result

                    elif self.mode == "normal":

                        self.print(
                            "9. MODE: NORMAL, SUBMODES:\n\n",
                            1,
                        )

                        self.results[task_key] = result

                    self.bar.report(len(self.results))

                    # Remove the task from pending status.
                    self._pending_tasks.pop(task_key, None)

                elif reply_payload[0] == TASK_FAILURE_SIGNAL:

                    self.print(f"8. RECEIVED IN CLIENT FROM SERVER: {reply}\n", 2)
                    self.print(
                        "8. RECEIVED IN CLIENT FROM SERVER: [task_key, task_failure_signal, error]\n\n",
                        1,
                    )

                    error_msg = msgpack.unpackb(reply_payload[1], raw=False)

                    self._handle_failed_tasks(task_key, error_msg)

                # If worker failure message was sent from this client to the server (refer below)
                # 1. restart the number of failed worker processes
                # 2. retry the unfinished tasks as needed.
                elif reply_payload[0] == WORKER_FAILURE_SIGNAL:

                    self.print(f"8. RECEIVED IN CLIENT FROM SERVER: {reply}\n", 2)
                    self.print(
                        "8. RECEIVED IN CLIENT FROM SERVER: [dummy_task_key, worker_failure_signal, error]\n\n",
                        1,
                    )

                    failed_task_keys = msgpack.unpackb(reply_payload[1], raw=False)

                    for failed_task_key in failed_task_keys:

                        warning_msg = "task id {} failed due to: worker death".format(
                            failed_task_key
                        )
                        warnings.warn(warning_msg, WORKER_FAILED)

                        self._handle_failed_tasks(
                            failed_task_key, "Premature worker death"
                        )

            # Check the status of the child processes every REQUEST_TIMEOUT milliseconds.
            worker_deaths = [not proc.is_alive() for proc in self.worker_procs]

            # If any workers are dead, inform the server and start that many workers again.
            if any(worker_deaths):

                # Update the worker process list.
                self.worker_procs = [
                    proc for proc in self.worker_procs if proc.is_alive()
                ]

                frames = [DUMMY_TASK_KEY, WORKER_FAILURE_SIGNAL]

                # The DEALER socket will prepend the client address.
                # [dummy_task_key, worker_failure_signal]
                self.client.send_multipart(frames)

                self.start_workers(sum(worker_deaths))

    def get(self, keyed=False):

        self.bar = ProgressBar()
        self.bar.set_total(len(self._task_keys))
        self.bar.report(0)

        self._poll()

        if len(self.results) == len(self._task_keys):

            self.bar.completion_report()

        self.close()

        if self.mode == "normal":

            if not keyed:
                return list(self.results.values())
            else:
                return self.results

        elif self.mode == "pandas":

            return self.dataframe
