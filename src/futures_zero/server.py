import zmq
import time
from multiprocessing import Process
from collections import OrderedDict
import msgpack

from .config import *


class Worker(object):
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

class WorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()
        self.busy_workers = list()
        self.all_workers = dict()

    def ready(self, worker_address):

        self.queue.pop(worker_address, None)
        self.queue[worker_address] = Worker(worker_address)

        # if worker_address not in self.all_workers:
        self.all_workers[worker_address] = self.queue[worker_address]

    def busy(self, worker_address):
        """An extra contraption to fix the bug in the original code.
        The edge case bug occurs when a worker is given a task request
        but on the same cycle sends back heartbeat. This will cause the
        server to expire the worker if the task takes too long.

        """
        self.busy_workers.append(worker_address)

    def free(self, worker_address):

        self.busy_workers.remove(worker_address)

    def purge(self):
        """Look for & kill expired workers."""
        t = time.time()
        expired = []

        for address, worker in self.all_workers.items():

            if t > worker.expiry:  # Worker expired
                expired.append(address)

        for address in expired:

            print("W: Idle worker expired: %s" % address)
            self.queue.pop(address, None)
            self.all_workers.pop(address, None)

        return expired

    def next(self):
        address, worker = self.queue.popitem(False)
        return address


class ServerProcess(Process):
    
    def __init__(self, client_address, verbose):
        super(ServerProcess, self).__init__()

        self.client_address = client_address
        self.verbose = verbose

    def print(self, s, lvl):

        if (self.verbose==-1) & (lvl==-1):
            print(s)

        if self.verbose>=lvl:
            print(s)

    def run(self):
        
        context = zmq.Context(1)

        # Frontend server socket (to talk to client)
        frontend = context.socket(zmq.ROUTER) 
        frontend.bind(SERVER_ENDPOINT) 

        # Backend server socket (to talk to workers)
        backend = context.socket(zmq.ROUTER)  
        backend.bind(WORKER_ENDPOINT) 

        # To listen to workers
        poll_workers = zmq.Poller()
        poll_workers.register(backend, zmq.POLLIN)

        # To listen to both workers and client
        poll_both = zmq.Poller()
        poll_both.register(frontend, zmq.POLLIN)
        poll_both.register(backend, zmq.POLLIN)

        # Queue of LRU workers ("Least Recently Used")
        workers = WorkerQueue()

        heartbeat_at = time.time() #+ HEARTBEAT_INTERVAL
        
        try:

            while True:
                
                # If workers queue is empty, we wait for workers to send the initial
                # WORKER_READY_SIGNAL signal so we can queue the ready workers.
                if len(workers.queue) > 0:
                    poller = poll_both
                else:
                    poller = poll_workers

                # Start listening to client/workers (blocking)
                socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))

                # Get message from the frontend client.
                if socks.get(frontend) == zmq.POLLIN:

                    # The DEALER socket prepended the client address.
                    # [client_address, task_key, task_mode_signal, start_method_signal, func_statefulness_signal, func, args] or
                    # [client_address, task_key, kill_signal]
                    frames = frontend.recv_multipart()

                    # Interrupted
                    if not frames:
                        self.print_1("2. RECEIVED IN SERVER FROM CLIENT: NULL\n\n")
                        break

                    if frames[2] in [
                        NORMAL_TASK_REQUEST_SIGNAL, 
                        PANDAS_PARTITION_TASK_REQUEST_SIGNAL, 
                        PANDAS_NONPARTITION_TASK_REQUEST_SIGNAL
                        ]:

                        self.print("2. RECEIVED IN SERVER FROM CLIENT: {}\n".format(frames), 2)
                        self.print("2. RECEIVED IN SERVER FROM CLIENT: [client_address, task_key, task_mode_signal, " \
                            "start_method_signal, func_statefulness_signal, func, args]\n\n", 1)

                        # Get the address of LRU worker
                        address = workers.next()

                        # Prepend the LRU worker address.
                        # [worker_address, client_address, task_key, task_mode_signal, start_method_signal, func_statefulness_signal, func, args]
                        frames.insert(0, address)

                        self.print("3. SENDING FROM SERVER TO WORKER: {}\n".format(frames), 2)
                        self.print("3. SENDING FROM SERVER TO WORKER: [worker_address, client_address, task_key, task_mode_signal, " \
                            "start_method_signal, func_statefulness_signal, func, args]\n\n", 1)

                        # The ROUTER will strip off the worker address from the frames and save it in 
                        # hash table, associating it with the worker socket.
                        # [worker_address, client_address, task_key, task_mode_signal, start_method_signal, func_statefulness_signal, func, args]
                        backend.send_multipart(frames)

                        workers.busy(address)

                    elif frames[2]==KILL_SIGNAL:

                        for worker_address in workers.all_workers.keys():

                            msg = [worker_address, KILL_SIGNAL]

                            # The ROUTER will strip off the worker address from the frames and save it in 
                            # hash table, associating it with the worker socket.
                            # [worker_address, kill_signal]
                            backend.send_multipart(msg)

                        break

                # Get message from the backend worker.
                if socks.get(backend) == zmq.POLLIN:
                    
                    # [worker_address, client_address, task_key, task_success_signal, result] or
                    # [worker_address, client_address, task_key, task_failure_signal, error] or
                    # [worker_address, heartbeat_signal] or
                    # [worker_address, worker_ready_signal] when it is first created.
                    # The DEALER socket prepended the worker address.
                    frames = backend.recv_multipart()

                    self.print("6. RECEIVED IN SERVER FROM WORKER RAW: {}\n\n".format(frames), 3)

                    # Interrupted.
                    if not frames:
                        break

                    # Strip the prepended worker address.
                    address = frames[0]

                    # The worker reply payload.
                    # [client_address, task_key, task_success_signal, result] or
                    # [client_address, task_key, task_failure_signal, error] or
                    # [heartbeat_signal] or
                    # [worker_ready_signal]
                    reply_payload = frames[1:]

                    if len(reply_payload) == 1:

                        if reply_payload[0]==WORKER_READY_SIGNAL:

                            workers.ready(address)

                        elif reply_payload[0]==HEARTBEAT_SIGNAL:

                            # print('RECEIVED HEARTBEAT FROM WORKER {}'.format(address))

                            # Ignore this heartbeat if the worker is already busy with a task.
                            if address not in workers.busy_workers:

                                workers.ready(address)

                            else:

                                print('IGNORING ++++++{}'.format(address))


                        else:

                            print("E: Invalid message from worker: %s" % reply_payload)

                    else:

                        if frames[3]==TASK_SUCCESS_SIGNAL:

                            self.print("6. RECEIVED IN SERVER FROM WORKER: {}\n".format(frames), 2)
                            self.print("6. RECEIVED IN SERVER FROM WORKER: [worker_address, client_address, task_key, task_success_signal, result]\n\n", 1)

                            self.print("7. SENDING FROM SERVER TO CLIENT: {}\n".format(reply_payload), 2)
                            self.print("7. SENDING FROM SERVER TO CLIENT: [client_address, task_key, task_success_signal, result]\n\n", 1)

                        if frames[3]==NUMPY_TASK_SUCCESS_SIGNAL:

                            self.print("6. RECEIVED IN SERVER FROM WORKER: {}\n".format(frames), 2)
                            self.print("6. RECEIVED IN SERVER FROM WORKER: [worker_address, client_address, task_key, numpy_task_success_signal, metadata, result]\n\n", 1)

                            self.print("7. SENDING FROM SERVER TO CLIENT: {}\n".format(reply_payload), 2)
                            self.print("7. SENDING FROM SERVER TO CLIENT: [client_address, task_key, numpy_task_success_signal, metadata, result]\n\n", 1)

                        if frames[3]==TASK_FAILURE_SIGNAL:

                            self.print("6. RECEIVED IN SERVER FROM WORKER: {}\n".format(frames), 1)
                            self.print("6. RECEIVED IN SERVER FROM WORKER: [worker_address, client_address, task_key, task_failure_signal, error]\n\n", 1)

                            self.print("7. SENDING FROM SERVER TO CLIENT: {}\n".format(reply_payload), 1)
                            self.print("7. SENDING FROM SERVER TO CLIENT: [client_address, task_key, task_failure_signal, error]\n\n", 1)

                        workers.free(address)
                        workers.ready(address)

                        # [client_address, task_key, task_success_signal, result] or
                        # [client_address, task_key, task_failure_signal, error]
                        # The ROUTER socket will strip off the client address.
                        frontend.send_multipart(reply_payload)

                    # Send heartbeats to idle workers if it's time.
                    if time.time() >= heartbeat_at:

                        for worker_address, worker in workers.queue.items():

                            msg = [worker_address, HEARTBEAT_SIGNAL]
                            
                            # [worker_address, heartbeat_signal]
                            # The ROUTER socket will strip off the worker address.
                            backend.send_multipart(msg)

                        # Update the next heartbeat time.
                        heartbeat_at = time.time() + HEARTBEAT_INTERVAL

                # Detect the dead workers that did not send heartbeat for a fixed period of time.
                dead_worker_addresses = workers.purge()

                if len(dead_worker_addresses) > 0:


                    self.print("6. DEAD WORKER(S) DETECTED: {}\n".format(dead_worker_addresses), 1)

                    binary_num_dead_workers = msgpack.packb(len(dead_worker_addresses), use_bin_type=True)
                    dummy_task_key = msgpack.packb(-1, use_bin_type=True)

                    frames = [self.client_address, dummy_task_key, WORKER_FAILURE_SIGNAL, binary_num_dead_workers]

                    self.print("7. SENDING FROM SERVER TO CLIENT: {}\n".format(frames), 1)
                    self.print("7. SENDING FROM SERVER TO CLIENT: [client_address, dummy_task_key, worker_failure_signal, num_dead_workers]\n\n", 1)

                    # The ROUTER socket will strip off the client address.
                    # [client_address, dummy_task_key, worker_failure_signal, num_dead_workers] 
                    frontend.send_multipart(frames)    

        except KeyboardInterrupt:
            
            
            print('E : Keyboard Interrupted 444')
            
        finally: 
            
            poll_both.unregister(frontend)
            poll_both.unregister(backend)

            poll_workers.unregister(backend)

            frontend.setsockopt(zmq.LINGER, 0)
            frontend.close()
            
            backend.setsockopt(zmq.LINGER, 0)
            backend.close()
            
            context.term()
                
