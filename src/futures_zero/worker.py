import os
import sys
import time
import warnings
from multiprocessing import Process
from random import randint

import cloudpickle
import feather
import msgpack
import numpy as np
import pandas as pd
import zmq
from zmq.error import ZMQError

from .config import *


class TASK_FAILED(UserWarning):
    pass


def worker_socket(context, poller):
    """Helper function that returns a new configured socket
    connected to the Paranoid Pirate queue"""
    identity = str(os.getpid()).encode()
    worker = context.socket(zmq.DEALER)  # DEALER ~ requester
    worker.setsockopt(zmq.IDENTITY, identity)
    poller.register(worker, zmq.POLLIN)
    worker.connect(WORKER_ENDPOINT)
    worker.send(WORKER_READY_SIGNAL)
    return identity, worker


class WorkerProcess(Process):
    """The process for the worker client that connects to the server and carries
    out the requested computations in parallel. The user function can access its
    state if needed. The underscore is for when the user wishes to subclass the
    BaseWorker.

    Parameters
    ----------
    verbose : int
        Available input values:
        - 0: no output
        - 1: show message titles
        - 2: show message contents

    dataframe : Pandas dataframe or None
        Used if ``apply`` method is invoked.
    """

    def __init__(
        self,
        __verbose__,
        __dataframe__=None,
        __forked__=True,
        __mode__="normal",
        __partition__=False,
        *args,
        **kwargs
    ):
        super(WorkerProcess, self).__init__()

        self.__verbose__ = __verbose__

        if __mode__ == "pandas":

            if __forked__:

                self.__dataframe__ = __dataframe__

            else:

                if __partition__:

                    pass

                else:

                    self.__dataframe__ = feather.read_dataframe(__dataframe__)

        else:

            self.__dataframe__ = None

    def print(self, s, lvl):

        if self.__verbose__ >= lvl:
            print(s)

    def run(self):

        context = zmq.Context(1)
        poller = zmq.Poller()

        identity, worker = worker_socket(context, poller)
        self.print("WORKER STARTED: {}".format(identity), 1)

        try:

            # Flag for the ``finally`` clause.
            perform_final = True

            while True:

                socks = dict(poller.poll())

                # Get message from the proxy server.
                if socks.get(worker) == zmq.POLLIN:

                    # The ROUTER socket strips the worker_address.
                    # [client_address, task_key, task_mode_signal, start_method_signal, func_statefulness_signal, func, args] or
                    # for normal task request.
                    # [kill_signal]
                    # if ``close`` is invoked from futures client.
                    frames = worker.recv_multipart()

                    # Interrupted
                    if not frames:
                        self.print(
                            "4. RECEIVED IN WORKER {} FROM SERVER: NULL\n\n".format(
                                identity
                            ),
                            1,
                        )
                        break

                    # Kill signal from the frontend client.
                    if len(frames) == 1 and frames[0] == KILL_SIGNAL:
                        break

                    # Task request from the frontend client.
                    elif frames[2] in [
                        NORMAL_TASK_REQUEST_SIGNAL,
                        PANDAS_PARTITION_TASK_REQUEST_SIGNAL,
                        PANDAS_NONPARTITION_TASK_REQUEST_SIGNAL,
                    ]:

                        self.print(
                            "4. RECEIVED IN WORKER {} FROM SERVER: {}\n".format(
                                identity, frames
                            ),
                            2,
                        )
                        self.print(
                            "4. RECEIVED IN WORKER {} FROM SERVER: [client_address, task_key, task_mode_signal, start_method_signal, "
                            "func_statefulness_signal, func, args]\n\n".format(
                                identity
                            ),
                            1,
                        )

                        # [client_address, task_key]
                        reply_frame_header = frames[:2]
                        # [task_mode_signal, start_method_signal, func_statefulness_signal]
                        task_properties = frames[2:5]
                        # [func, args]
                        task_payload = frames[5:]

                        func_binary = task_payload[0]
                        func = cloudpickle.loads(func_binary)

                        args_binary = task_payload[1]
                        args, kwargs = msgpack.unpackb(args_binary, raw=False)

                        self.print("ARGS: {}\n\n".format(args), 3)
                        self.print("KWARGS: {}\n\n".format(kwargs), 3)

                        # Try to catch user function exception without disturbing the process if possible.
                        try:

                            # Run the user ``func``.
                            if task_properties[0] == NORMAL_TASK_REQUEST_SIGNAL:

                                if task_properties[2] == STATEFUL_METHOD_SIGNAL:

                                    self.print("MODE: NORMAL, STATEFUL", 1)

                                    result = func(self, *args, **kwargs)

                                elif task_properties[2] == STATELESS_METHOD_SIGNAL:

                                    result = func(*args, **kwargs)

                            elif (
                                task_properties[0]
                                == PANDAS_PARTITION_TASK_REQUEST_SIGNAL
                            ):

                                # If we are using ``apply`` method, the ``func`` is required to have the
                                # dataframe as its first positional argument.
                                result = func(self.__dataframe__, *args, **kwargs)

                                if isinstance(result, (pd.DataFrame, pd.Series)):
                                    result = result.values

                            elif (
                                task_properties[0]
                                == PANDAS_NONPARTITION_TASK_REQUEST_SIGNAL
                            ):

                                # If we are using ``apply`` method, the ``func`` is required to have the
                                # dataframe as its first positional argument.
                                result = func(self.__dataframe__, *args, **kwargs)

                                if isinstance(result, (pd.DataFrame, pd.Series)):
                                    result = result.values

                            else:

                                raise ValueError("Invalid task_model_signal.")

                            # Communicate the results back to the server.
                            # If the result is a numpy n-dim array, use memory buffer for messaging.
                            if isinstance(result, (np.ndarray, np.generic)):

                                if not result.flags["C_CONTIGUOUS"]:
                                    result = np.ascontiguousarray(result)

                                metadata = dict(
                                    dtype=str(result.dtype),
                                    shape=result.shape,
                                )
                                metadata = msgpack.packb(metadata, use_bin_type=True)

                                reply_frame = reply_frame_header + [
                                    NUMPY_TASK_SUCCESS_SIGNAL,
                                    metadata,
                                    result,
                                ]

                                self.print(
                                    "5. SENDING FROM WORKER {} TO SERVER: {}\n".format(
                                        identity, reply_frame
                                    ),
                                    2,
                                )
                                self.print(
                                    "5. SENDING FROM WORKER {} TO SERVER: [client_address, task_key, numpy_task_success_signal, "
                                    "metadata, result]\n\n".format(identity),
                                    1,
                                )

                            # If not, use the msgpack to serialize the return data.
                            else:

                                result_binary = msgpack.packb(result, use_bin_type=True)

                                # [client_address, task_key, task_success_signal, result]
                                reply_frame = reply_frame_header + [
                                    TASK_SUCCESS_SIGNAL,
                                    result_binary,
                                ]

                                self.print(
                                    "5. SENDING FROM WORKER {} TO SERVER: {}\n".format(
                                        identity, reply_frame
                                    ),
                                    2,
                                )
                                self.print(
                                    "5. SENDING FROM WORKER {} TO SERVER: [client_address, task_key, task_success_signal, "
                                    "result]\n\n".format(identity),
                                    1,
                                )

                        except Exception as e:

                            task_key_bin = reply_frame_header[-1]
                            task_key = msgpack.unpackb(task_key_bin, raw=False)

                            warning_msg = "task id {} failed due to: {}".format(
                                task_key, repr(e)
                            )
                            warnings.warn(warning_msg, TASK_FAILED)

                            error_binary = msgpack.packb(repr(e), use_bin_type=True)

                            # [client_address, task_key, task_failure_signal, error]
                            reply_frame = reply_frame_header + [
                                TASK_FAILURE_SIGNAL,
                                error_binary,
                            ]

                            self.print(
                                "5. SENDING FROM WORKER {} TO SERVER: {}\n".format(
                                    identity, reply_frame
                                ),
                                1,
                            )
                            self.print(
                                "5. SENDING FROM WORKER {} TO SERVER: [client_address, task_key, task_failure_signal, "
                                "error]\n\n".format(identity),
                                1,
                            )

                        # The DEALER socket will prepend the worker address.
                        # [client_address, task_key, task_success_signal, result] or
                        # [client_address, task_key, numpy_task_success_signal, result] or
                        # [client_address, task_key, task_failure_signal, error]
                        worker.send_multipart(reply_frame)

                    else:

                        raise ValueError("Invalid message")

        except KeyboardInterrupt:

            print("Keyboard Interrupted")

            perform_final = True

        except ZMQError:

            perform_final = False

        except Exception as e:

            print("Worker Process Internal Failure: ", repr(e))

            reply_frame = frames[:2] + [TASK_FAILURE_SIGNAL] + task_payload

            worker.send_multipart(reply_frame)

            perform_final = True

        finally:

            self.print("GRACEFULLY TERMINATING WORKER {}".format(identity), 1)

            if perform_final:

                poller.unregister(worker)
                worker.setsockopt(zmq.LINGER, 0)
                worker.close()
                context.term()
