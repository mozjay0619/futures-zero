import yaml
from multiprocessing import Process
import zmq
import time
from random import randint
import numpy as np
import cloudpickle
import msgpack
import sys
import numpy as np

from .config import *

import warnings


class TASK_FAILED(UserWarning):
    pass



def worker_socket(context, poller):
    """Helper function that returns a new configured socket
       connected to the Paranoid Pirate queue"""
    worker = context.socket(zmq.DEALER) # DEALER ~ requester
    identity = b"%04X-%04X" % (randint(0, 0x10000), randint(0, 0x10000))
    worker.setsockopt(zmq.IDENTITY, identity)
    poller.register(worker, zmq.POLLIN)
    worker.connect(WORKER_ENDPOINT)
    worker.send(WORKER_READY_SIGNAL)
    return identity, worker


class WorkerProcess(Process):

    def __init__(self, __verbose__, __dataframe__=None, out_dim=None):
        super(WorkerProcess, self).__init__()

        self.verbose = __verbose__
        self.dataframe = __dataframe__

    def print(self, s, lvl):

        if (self.verbose==-1) & (lvl==-1):
            print(s)

        if self.verbose>=lvl:
            print(s)

    def run(self):

        context = zmq.Context(1)
        poller = zmq.Poller()

        liveness = HEARTBEAT_LIVENESS
        interval = INTERVAL_INIT
        heartbeat_at = time.time() + HEARTBEAT_INTERVAL

        identity, worker = worker_socket(context, poller)
        cycles = 0

        try:
        
            while True:

                socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))

                # Get message from the proxy server. 
                if socks.get(worker) == zmq.POLLIN:

                    # The ROUTER socket strips the worker_address.
                    # [client_address, task_key, task_mode_signal, start_method_signal, func_statefulness_signal, func, args] or
                    # [kill_signal] or
                    # [heartbeat_signal] from the proxy server.
                    frames = worker.recv_multipart()
                    
                    # Interrupted
                    if not frames:
                        self.print("4. RECEIVED IN WORKER {} FROM SERVER: NULL\n\n".format(identity), 1)
                        break 

                    # Regular heartbeat from the proxy server to check that the server is alive.
                    if len(frames)==1 and frames[0]==HEARTBEAT_SIGNAL:
                        liveness = HEARTBEAT_LIVENESS

                    # Kill signal from the frontend client.
                    elif len(frames)==1 and frames[0]==KILL_SIGNAL:
                        break

                    # Task request from the frontend client.
                    elif frames[2] in [
                        NORMAL_TASK_REQUEST_SIGNAL, 
                        PANDAS_PARTITION_TASK_REQUEST_SIGNAL, 
                        PANDAS_NONPARTITION_TASK_REQUEST_SIGNAL
                        ]:

                        self.print("4. RECEIVED IN WORKER {} FROM SERVER: {}\n".format(identity, frames), 2)
                        self.print("4. RECEIVED IN WORKER {} FROM SERVER: [client_address, task_key, task_mode_signal, start_method_signal, " \
                            "func_statefulness_signal, func, args]\n\n".format(identity), 1)

                        reply_frame_header = frames[:2]  # [client_address, task_key]
                        task_properties = frames[2:5]  # [task_mode_signal, start_method_signal, func_statefulness_signal]
                        task_payload = frames[5:]  # [func, args]

                        func_binary = task_payload[0]
                        func = cloudpickle.loads(func_binary)

                        args_binary = task_payload[1]
                        args, kwargs = msgpack.unpackb(args_binary, raw=False)

                        # Try to catch user function exception without disturbing the process if possible.
                        try:

                            if task_properties[0]==NORMAL_TASK_REQUEST_SIGNAL:

                                if task_properties[2]==STATEFUL_METHOD_SIGNAL:

                                    result = func(self, *args, **kwargs)

                                elif task_properties[2]==STATELESS_METHOD_SIGNAL:

                                    result = func(*args, **kwargs)

                            elif task_properties[0]==PANDAS_PARTITION_TASK_REQUEST_SIGNAL:

                                if task_properties[1]==FORKED_PROCESS_SIGNAL:

                                    # If we are using ``apply`` method, the ``func`` is required to have the 
                                    # dataframe as its first positional argument.
                                    result = func(self.dataframe, *args, **kwargs)

                                elif task_properties[1]==SPAWNED_PROCESS_SIGNAL:

                                    # read and then pas sin
                                    pass

                            elif task_properties[0]==PANDAS_NONPARTITION_TASK_REQUEST_SIGNAL:

                                if task_properties[1]==FORKED_PROCESS_SIGNAL:

                                    # If we are using ``apply`` method, the ``func`` is required to have the 
                                    # dataframe as its first positional argument.
                                    result = func(self.dataframe, *args, **kwargs)


                            else:

                                raise ValueError('Invalid task_model_signal.')
                            
                            if isinstance(result, np.ndarray):

                                metadata = dict(
                                    dtype = str(result.dtype),
                                    shape = result.shape,
                                )
                                metadata = msgpack.packb(metadata, use_bin_type=True)

                                reply_frame = reply_frame_header + [NUMPY_TASK_SUCCESS_SIGNAL, metadata, result]

                                self.print("5. SENDING FROM WORKER {} TO SERVER: {}\n".format(identity, reply_frame), 2)
                                self.print("5. SENDING FROM WORKER {} TO SERVER: [client_address, task_key, numpy_task_success_signal, " \
                                    "metadata, result]\n\n".format(identity), 1)

                            # If not, use the msgpack to serialize the return data.
                            else:

                                result_binary = msgpack.packb(result, use_bin_type=True)

                                # [client_address, task_key, task_success_signal, result]
                                reply_frame = reply_frame_header + [TASK_SUCCESS_SIGNAL, result_binary]

                                # # [client_address, task_key, task_success_signal, result] 
                                # # The DEALER socket will prepend the worker address.
                                # worker.send_multipart(reply_frame)

                                self.print("5. SENDING FROM WORKER {} TO SERVER: {}\n".format(identity, reply_frame), 2)
                                self.print("5. SENDING FROM WORKER {} TO SERVER: [client_address, task_key, task_success_signal, " \
                                    "result]\n\n".format(identity), 1)

                        except Exception as e:

                            task_key_bin = reply_frame_header[-1]
                            task_key = msgpack.unpackb(task_key_bin, raw=False)

                            template = "task id {} failed due to: {}"
                            warning_msg = template.format(task_key, repr(e))

                            warnings.warn(warning_msg, TASK_FAILED)

                            error_binary = msgpack.packb(repr(e), use_bin_type=True)

                            # [client_address, task_key, task_failure_signal, error]
                            reply_frame = reply_frame_header + [TASK_FAILURE_SIGNAL, error_binary] 

                            self.print("5. SENDING FROM WORKER {} TO SERVER: {}\n".format(identity, reply_frame), 1)
                            self.print("5. SENDING FROM WORKER {} TO SERVER: [client_address, task_key, task_failure_signal, " \
                                "error]\n\n".format(identity), 1)

                        # [client_address, task_key, task_success_signal, result] or
                        # [client_address, task_key, numpy_task_success_signal, result] or
                        # [client_address, task_key, task_failure_signal, error]
                        # The DEALER socket will prepend the worker address.
                        worker.send_multipart(reply_frame)

                        liveness = HEARTBEAT_LIVENESS

                    else:
                        print("E: Invalid message: ! %s" % frames)

                    interval = INTERVAL_INIT


                else:

                    liveness -= 1

                    if liveness == 0:

                        print("W: Heartbeat failure, can't reach queue")
                        print("W: Reconnecting in %0.2fs..." % interval)
                        time.sleep(interval)

                        if interval < INTERVAL_MAX:
                            interval *= 2
                        poller.unregister(worker)
                        worker.setsockopt(zmq.LINGER, 0)
                        worker.close()
                        identity, worker  = worker_socket(context, poller)
                        print('OH SHIT')
                        liveness = HEARTBEAT_LIVENESS
                        
                if time.time() > heartbeat_at:

                    worker.send(HEARTBEAT_SIGNAL)

                    self.print("5. SENDING FROM WORKER {} TO SERVER: HEARTBEAT\n".format(identity), -1)

                    heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        
        except KeyboardInterrupt:
            

            print('E: Keyboard Interrupted')
        
        except Exception as e:
            
            print('Exception', str(e))

            reply_frame = frames[:2] + [TASK_FAILURE_SIGNAL] + task_payload

            worker.send_multipart(reply_frame)
            
        finally:
            
            poller.unregister(worker)
            worker.setsockopt(zmq.LINGER, 0)
            worker.close()
            context.term()
            

