import time
from collections import OrderedDict
from multiprocessing import Process

import msgpack
import zmq

from .config import *
from .utils import check_pid


class WorkerQueue(object):
    """Worker queue used by the server for load balancing. It uses
    the list data structure for queue. It also keeps track of which
    worker is busy processing which task key. The set of all worker
    address is used by, for instance, kill command.
    """

    def __init__(self):
        self.free_workers = list()
        self.busy_workers = dict()
        self.all_workers = set()

    def ready(self, worker_address):

        if worker_address in self.free_workers:
            self.free_workers.remove(worker_address)

        self.free_workers.append(worker_address)
        self.all_workers.add(worker_address)

    def busy(self, worker_address, task_key):

        self.busy_workers[worker_address] = task_key

    def free(self, worker_address):

        self.busy_workers.pop(worker_address, None)

    def next(self):

        address = self.free_workers.pop(False)
        return address

    def purge(self):
        """Only called when the client detects dead worker processes.
        It purges any record of the dead worker from the queue's state.
        Also, it will retrieve the keys of tasks that those dead workers were
        unable to finish processing in ``unfinished_tasks`` and pass
        them along to the client for further instructions.
        """
        unfinished_tasks = []

        all_workers = self.all_workers.copy()
        for address in all_workers:

            pid = int(address.decode())
            if not check_pid(pid):

                if address in self.free_workers:
                    self.free_workers.remove(address)

                if address in self.busy_workers:
                    task_key = self.busy_workers.pop(address)
                    unfinished_tasks.append(task_key)

                if address in self.all_workers:
                    self.all_workers.remove(address)

        return unfinished_tasks


class ServerProcess(Process):
    """The Proxy Server that the Futures client and worker clients connect to.
    It uses asynchronous messaging queues to communicate with clients. Its main
    functionality is load balancing, which is implemented as a queue of LRU workers
    (refer to ``WorkerQueue``).

    Parameters
    ----------
    client_address : str

    verbose : int
    """

    def __init__(self, client_address, verbose):
        super(ServerProcess, self).__init__()

        self.client_address = client_address
        self.verbose = verbose

    def print(self, s, lvl):

        if self.verbose >= lvl:
            print(s)

    def run(self):

        context = zmq.Context(1)

        # Frontend server socket (to talk to client)
        frontend = context.socket(zmq.ROUTER)
        frontend.bind(SERVER_ENDPOINT)

        # Backend server socket (to talk to workers)
        backend = context.socket(zmq.ROUTER)
        backend.bind(WORKER_ENDPOINT)

        # Poller for workers
        poll_workers = zmq.Poller()
        poll_workers.register(backend, zmq.POLLIN)

        # Poller for both workers and client
        poll_both = zmq.Poller()
        poll_both.register(frontend, zmq.POLLIN)
        poll_both.register(backend, zmq.POLLIN)

        # Queue of LRU workers
        workers = WorkerQueue()

        try:

            # Flag for the ``finally`` clause.
            perform_final = True

            while True:

                # If workers queue is empty, we wait for workers to send the initial
                # WORKER_READY_SIGNAL signal so we can queue the ready workers.
                if len(workers.free_workers) > 0:
                    poller = poll_both
                else:
                    poller = poll_workers

                # Start listening to client/workers (blocking)
                socks = dict(poller.poll())

                # Get message from the backend worker.
                if socks.get(backend) == zmq.POLLIN:

                    # [worker_address, client_address, task_key, task_success_signal, result] or
                    # [worker_address, client_address, task_key, task_failure_signal, error] or
                    # [worker_address, worker_ready_signal]
                    # The DEALER socket prepended the worker address.
                    frames = backend.recv_multipart()

                    self.print(
                        "6. RECEIVED IN SERVER FROM WORKER RAW: {}\n\n".format(frames),
                        3,
                    )

                    # Interrupted.
                    if not frames:
                        break

                    # Strip the prepended worker address.
                    address = frames[0]

                    # The worker reply payload.
                    # [client_address, task_key, task_success_signal, result] or
                    # [client_address, task_key, task_failure_signal, error] or
                    # [worker_ready_signal]
                    reply_payload = frames[1:]

                    if len(reply_payload) == 1:

                        if reply_payload[0] == WORKER_READY_SIGNAL:

                            workers.ready(address)

                        else:

                            print("E: Invalid message from worker: %s" % reply_payload)

                    else:

                        if frames[3] == TASK_SUCCESS_SIGNAL:

                            self.print(
                                "6. RECEIVED IN SERVER FROM WORKER: {}\n".format(
                                    frames
                                ),
                                2,
                            )
                            self.print(
                                "6. RECEIVED IN SERVER FROM WORKER: [worker_address, client_address, task_key, task_success_signal, result]\n\n",
                                1,
                            )

                            self.print(
                                "7. SENDING FROM SERVER TO CLIENT: {}\n".format(
                                    reply_payload
                                ),
                                2,
                            )
                            self.print(
                                "7. SENDING FROM SERVER TO CLIENT: [client_address, task_key, task_success_signal, result]\n\n",
                                1,
                            )

                        if frames[3] == NUMPY_TASK_SUCCESS_SIGNAL:

                            self.print(
                                "6. RECEIVED IN SERVER FROM WORKER: {}\n".format(
                                    frames
                                ),
                                2,
                            )
                            self.print(
                                "6. RECEIVED IN SERVER FROM WORKER: [worker_address, client_address, task_key, numpy_task_success_signal, metadata, result]\n\n",
                                1,
                            )

                            self.print(
                                "7. SENDING FROM SERVER TO CLIENT: {}\n".format(
                                    reply_payload
                                ),
                                2,
                            )
                            self.print(
                                "7. SENDING FROM SERVER TO CLIENT: [client_address, task_key, numpy_task_success_signal, metadata, result]\n\n",
                                1,
                            )

                        if frames[3] == TASK_FAILURE_SIGNAL:

                            self.print(
                                "6. RECEIVED IN SERVER FROM WORKER: {}\n".format(
                                    frames
                                ),
                                1,
                            )
                            self.print(
                                "6. RECEIVED IN SERVER FROM WORKER: [worker_address, client_address, task_key, task_failure_signal, error]\n\n",
                                1,
                            )

                            self.print(
                                "7. SENDING FROM SERVER TO CLIENT: {}\n".format(
                                    reply_payload
                                ),
                                1,
                            )
                            self.print(
                                "7. SENDING FROM SERVER TO CLIENT: [client_address, task_key, task_failure_signal, error]\n\n",
                                1,
                            )

                        # Remove the worker from busy list.
                        workers.free(address)
                        workers.ready(address)

                        # [client_address, task_key, task_success_signal, result] or
                        # [client_address, task_key, task_failure_signal, error]
                        # The ROUTER socket will strip off the client address.
                        frontend.send_multipart(reply_payload)

                # Get message from the frontend client.
                if socks.get(frontend) == zmq.POLLIN:

                    # The DEALER socket prepended the client address.
                    # [client_address, task_key, task_mode_signal, start_method_signal, func_statefulness_signal, func, args]
                    # for normal task request.
                    # [client_address, dummy_task_key, kill_signal]
                    # if ``close`` is invoked from futures client.
                    # [client_address, dummy_task_key, worker_failure_signal]
                    # if any worker process termination is detected from futures client.
                    frames = frontend.recv_multipart()

                    self.print(
                        "2. RECEIVED IN SERVER FROM CLIENT RAW: {}\n\n".format(frames),
                        3,
                    )

                    # Interrupted
                    if not frames:
                        self.print("2. RECEIVED IN SERVER FROM CLIENT: NULL\n\n", 1)
                        break

                    if frames[2] in [
                        NORMAL_TASK_REQUEST_SIGNAL,
                        PANDAS_PARTITION_TASK_REQUEST_SIGNAL,
                        PANDAS_NONPARTITION_TASK_REQUEST_SIGNAL,
                    ]:

                        self.print(
                            "2. RECEIVED IN SERVER FROM CLIENT: {}\n".format(frames), 2
                        )
                        self.print(
                            "2. RECEIVED IN SERVER FROM CLIENT: [client_address, task_key, task_mode_signal, "
                            "start_method_signal, func_statefulness_signal, func, args]\n\n",
                            1,
                        )

                        # Get the address of LRU worker
                        address = workers.next()

                        # Get the task key
                        task_key = msgpack.unpackb(frames[1], raw=False)

                        # Prepend the LRU worker address.
                        # [worker_address, client_address, task_key, task_mode_signal, start_method_signal, func_statefulness_signal, func, args]
                        frames.insert(0, address)

                        self.print(
                            "3. SENDING FROM SERVER TO WORKER: {}\n".format(frames), 2
                        )
                        self.print(
                            "3. SENDING FROM SERVER TO WORKER: [worker_address, client_address, task_key, task_mode_signal, "
                            "start_method_signal, func_statefulness_signal, func, args]\n\n",
                            1,
                        )

                        # The ROUTER will strip off the worker address from the frames and save it in
                        # hash table, associating it with the worker socket.
                        # [worker_address, client_address, task_key, task_mode_signal, start_method_signal, func_statefulness_signal, func, args]
                        backend.send_multipart(frames)

                        workers.busy(address, task_key)

                    elif frames[2] == KILL_SIGNAL:

                        self.print(
                            "*. RECEIVED IN SERVER FROM CLIENT: {}\n".format(frames), 2
                        )
                        self.print(
                            "*. RECEIVED IN SERVER FROM CLIENT: [client_address, dummy_task_key, kill_signal]\n\n",
                            1,
                        )

                        for worker_address in workers.all_workers:

                            msg = [worker_address, KILL_SIGNAL]

                            # The ROUTER will strip off the worker address from the frames and save it in
                            # hash table, associating it with the worker socket.
                            # [worker_address, kill_signal]
                            backend.send_multipart(msg)

                        break

                    elif frames[2] == WORKER_FAILURE_SIGNAL:

                        self.print(
                            "5. RECEIVED IN SERVER FROM CLIENT: {}\n".format(frames), 2
                        )
                        self.print(
                            "5. RECEIVED IN SERVER FROM CLIENT: [client_address, dummy_task_key, worker_failure_signal]\n\n",
                            1,
                        )

                        failed_task_keys = workers.purge()
                        failed_task_keys = msgpack.packb(
                            failed_task_keys, use_bin_type=True
                        )

                        msg = [
                            self.client_address,
                            DUMMY_TASK_KEY,
                            WORKER_FAILURE_SIGNAL,
                            failed_task_keys,
                        ]

                        frontend.send_multipart(msg)

        except KeyboardInterrupt:

            print("Keyboard Interrupted")

            perform_final = True

        except ZMQError:

            perform_final = False

        finally:

            if perform_final:

                poll_both.unregister(frontend)
                poll_both.unregister(backend)
                poll_workers.unregister(backend)

                frontend.setsockopt(zmq.LINGER, 0)
                frontend.close()

                backend.setsockopt(zmq.LINGER, 0)
                backend.close()

                context.term()
