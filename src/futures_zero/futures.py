import zmq
import itertools
import logging
import sys
import psutil
import cloudpickle
import msgpack
import time
from random import randint
from multiprocessing import set_start_method
import numpy as np
import feather
import os
from collections import defaultdict

from .config import *

from .server import ServerProcess
from .worker import WorkerProcess

from .utils import ProgressBar



class Futures:
	
	def __init__(self, n_workers=None, start_method='fork', verbose=0, worker=None, worker_args=[], worker_kwargs={}):

		self.n_workers = n_workers

		self.start_method = start_method

		self.mode = 'normal'

		self.verbose = verbose

		

		self.worker = worker
		self.worker_args = worker_args
		self.worker_kwargs = worker_kwargs

		if n_workers is None:

			self.n_workers = psutil.cpu_count(logical=False)

		self.server_client_online = False

		self.dataframe = None
		self.results = dict()
		self.errors = dict()
		self._task_keys = set()
		self._pending_tasks = dict()
		self._failed_tasks = dict()
		self._fail_counter = defaultdict(int)
		

	def print(self, s, lvl):

		if (self.verbose==-1) & (lvl==-1):
			print(s)

		if self.verbose>=lvl:
			print(s)

	def start_workers(self, n_workers):
		
		self.worker_procs = []


		
		for _ in range(n_workers):

			if self.worker is not None:

				worker_proc = self.worker(
					self.verbose,
					self.dataframe,
					*self.worker_args, 
					**self.worker_kwargs
					)

			else:

				worker_proc = WorkerProcess(
					__verbose__=self.verbose, 
					__dataframe__=self.dataframe)

			worker_proc.daemon = True
			worker_proc.start()
			
			self.worker_procs.append(worker_proc)
		
	def start_server(self):
		
		self.server_process = ServerProcess(self.client_address, self.verbose)
		self.server_process.daemon = True
		self.server_process.start()

	def start_client(self):

		self.context = zmq.Context()
		self.client = self.context.socket(zmq.DEALER)
		self.client_address = b"%04X-%04X" % (randint(0, 0x10000), randint(0, 0x10000))
		self.client.setsockopt(zmq.IDENTITY, self.client_address)
		self.client.connect(SERVER_ENDPOINT)

	def start(self):

		self.start_workers(self.n_workers)
		self.start_client()
		self.start_server()

		self.server_client_online = True

	def close(self):

		dummy_task_key = msgpack.packb(-1, use_bin_type=True)
		frames = [dummy_task_key, KILL_SIGNAL]

		# [task_key, kill_signal]
		# The DEALER socket will prepend the client address.
		self.client.send_multipart(frames)

		self.server_client_online = False

	def clean(self):

		self.dataframe = None
		self._task_keys = set()
		self._pending_tasks = dict()
		self.results = dict()
		self.errors = dict()
		self._failed_tasks = dict()
		
	

	def apply_to(self, dataframe, groupby=None, orderby=None, fork=True):
		"""Set up the dataframe 

		If fork is True and groupby is not None:

			dicts

			all the method must be parallelized for each group

		"""

		self.mode = 'pandas'

		if groupby:

			self.sub_mode=='partition'

			dataframe_dict = dict()

			for key, sub_df in dataframe.groupby(groupby):

				if orderby:

					sub_df = sub_df.sort_values(orderby)

				sub_df.reset_index(drop=True, inplace=True)
				sub_df = sub_df.copy(deep=False)

				dataframe_dict[key] = sub_df

			dataframe = dataframe_dict

		else:

			self.sub_mode = 'nonpartition'

			if orderby:

				dataframe = dataframe.sort_values(orderby)

				dataframe.reset_index(drop=True, inplace=True)
				dataframe = dataframe.copy(deep=False)



		if fork:

			set_start_method("fork", force=True)

			if groupby:

				self.dataframe = dataframe_dict
		
			else:

			
				self.dataframe = dataframe

		else:

			global global_dataframe
			global_dataframe = dataframe

			def write_dataframe(key=None):

				global global_dataframe

				if isinstance(global_dataframe, dict):

					
					filename = ''.join([TMP_FILENAME, str(key)])
					filepath = os.path.join(os.getcwd(), filename)
					
					feather.write_dataframe(global_dataframe[key], filepath)

					print(key)

				else:

					print(global_dataframe)

			if groupby:

				for key in dataframe.keys():

					self.submit_keyed(key, write_dataframe, key)



		


	def apply(self, func, column=None, *args, **kwargs):

		self.mode = 'pandas'

		key = column or len(self._task_keys)

		binary = self._serialize(key, func, *args, **kwargs)
		self._submit(key, binary)

	def capply(self, func, column=None, *args, **kwargs):

		pass

	def submit(self, func, *args, __key__=None, __stateful__=False, **kwargs):

		self.mode = 'normal'

		if __key__ is None:
			__key__ = len(self._task_keys)

		binary = self._serialize(__key__, __stateful__, func, *args, **kwargs)
		self._submit(__key__, binary)

	def _serialize(self, key, stateful, func, *args, **kwargs):

		# Serialize the task payload, comprised of the user method and method arguments.
		binary_func = cloudpickle.dumps(func)
		binary_args = msgpack.packb([args, kwargs], use_bin_type=True)
		binary_key = msgpack.packb(key, use_bin_type=True)

		task_ctrl_msg = []

		# Task mode message.
		if self.mode=='normal':
			task_ctrl_msg.append(NORMAL_TASK_REQUEST_SIGNAL)  # b"\x04"
		elif self.mode=='pandas':
			if self.sub_mode=='partition':
				task_ctrl_msg.append(PANDAS_PARTITION_TASK_REQUEST_SIGNAL)  # b"\x05"
			elif self.sub_mode=='nonpartition':
				task_ctrl_msg.append(PANDAS_NONPARTITION_TASK_REQUEST_SIGNAL)  # b"\x06"

		# Process start method message.
		if self.start_method=='fork':
			task_ctrl_msg.append(FORKED_PROCESS_SIGNAL)  # b"\x07"
		elif self.start_method in ['spawn', 'forkserver']:
			task_ctrl_msg.append(SPAWNED_PROCESS_SIGNAL)  # b"\x08"
		else:
			raise ValueError('Not a valid start method.')

		# User method statefulness message.
		if stateful:
			task_ctrl_msg.append(STATEFUL_METHOD_SIGNAL)  # b"\x10"
		else:
			task_ctrl_msg.append(STATELESS_METHOD_SIGNAL)  # b"\x11"

		return [binary_key] + task_ctrl_msg + [binary_func, binary_args]
		
	def _submit(self, key, request):

		if not self.server_client_online:
			self.start()
			time.sleep(0.05)
		
		self.print("1. SENDING FROM CLIENT TO SERVER: {}\n".format(request), 2)
		self.print("** NOTE: b'\\x92\\x90\\x80' is [[], {}]\n\n", 2)
		self.print("1. SENDING FROM CLIENT TO SERVER: [task_key, task_mode_signal, start_method_signal, " \
			"func_statefulness_signal, func, args]", 1)

		# The DEALER socket will prepend the client address.
		# [task_key, task_mode_signal, start_method_signal, func_statefulness_signal, func, args]
		self.client.send_multipart(request)  

		self._pending_tasks[key] = request
		self._task_keys.add(key)


	def get(self):

		self.bar = ProgressBar()

		self.bar.set_total(len(self._task_keys))
		self.bar.report(0)

		while len(self.results) + len(self.errors) < len(self._task_keys):

			retries_left = REQUEST_RETRIES
			
			# Start listening to replies from the server
			if (self.client.poll(REQUEST_TIMEOUT) & zmq.POLLIN) != 0:

				reply = self.client.recv_multipart()  
				# The REQ socket stripped off the client address.
				# [task_key, task_signal, error_msg, task_signal, task_signal, func, args] or
				# [task_key, task_signal, result] or
				# [dummy_task_key, worker_failure_signal, num_dead_workers] 

				task_key = msgpack.unpackb(reply[0], raw=False)  # task_key

				reply_payload = reply[1:]  
				# [task_success_signal, result]
				# [numpy_task_success_signal, metadata, result]
				# [task_signal, error_msg]
				# [worker_failure_signal, binary_num_dead_workers]

				if reply_payload[0]==TASK_SUCCESS_SIGNAL:

					self.print("8. RECEIVED FROM SERVER IN CLIENT: {}\n".format(reply), 2)
					self.print("8. RECEIVED FROM SERVER IN CLIENT: [task_key, task_success_signal, result]\n\n", 1)
					
					self._pending_tasks.pop(task_key, None)
					self.results[task_key] = msgpack.unpackb(reply_payload[1], raw=False)

					retries_left = REQUEST_RETRIES

				elif reply_payload[0]==NUMPY_TASK_SUCCESS_SIGNAL:

					self.print("8. RECEIVED IN CLIENT FROM SERVER: {}\n".format(reply), 2)
					self.print("8. RECEIVED IN CLIENT FROM SERVER: [task_key, numpy_task_success_signal, metadata, result]\n\n", 1)

					self.bar.report(len(self.results))
					self._pending_tasks.pop(task_key, None)
					metadata = msgpack.unpackb(reply_payload[1], raw=False)
					buf = memoryview(reply_payload[2])
					result = np.frombuffer(buf, dtype=metadata['dtype'])

					if self.mode=='pandas':

						if self.sub_mode=='nonpartition':

							if (isinstance(result, np.ndarray)) & (len(result)==len(self.dataframe)) & (not isinstance(task_key, int)):
					
								self.dataframe[task_key] = result

					elif self.mode=='normal':

						self.results[task_key] = result

				elif reply_payload[0]==TASK_FAILURE_SIGNAL:

					self.print("8. RECEIVED IN CLIENT FROM SERVER: {}\n".format(reply), 2)
					self.print("8. RECEIVED IN CLIENT FROM SERVER: [task_key, task_failure_signal, error]\n\n", 1)

					self._fail_counter[task_key] += 1

					if self._fail_counter[task_key] < REQUEST_RETRIES:

						self.print("9. TASK {} FAILED {} TIME(S), RETRYING\n\n".format(task_key, self._fail_counter[task_key]), 1)

						self._submit(task_key, self._pending_tasks.pop(task_key, None))

					else:

						self.print("9. TASK {} FAILED {} TIME(S), ABORTING\n\n".format(task_key, self._fail_counter[task_key]), 1)

						self._failed_tasks[task_key] = self._pending_tasks.pop(task_key, None)

						self.errors[task_key] = reply_payload[1]

				elif reply_payload[0]==WORKER_FAILURE_SIGNAL:

					n_dead_workers = msgpack.unpackb(reply_payload[1], raw=False)

					print('restarting workers')

					self.start_workers(n_dead_workers)

					returned_keys = list(self.errors.keys()) + list(self.results.keys())

					# print(returned_keys, '+++')

					# print(self._pending_tasks)

					unreturned_keys = [k for k, v in self._pending_tasks if k not in returned_keys]

					print('asdfasdfadsadsf')

					# print(unreturned_keys, '+++')

					# for task_key in unreturned_keys:

					# 	self.errors[task_key] = 'Dead worker'

					# 	self._failed_tasks[task_key] = self._pending_tasks.pop(task_key, None)

					# print('HO OHIST', unreturned_keys)
						
				

				# print('===')

				# retries_left -= 1
				# # logging.warning("No response from server")

				# print("we down here")
				# Socket is confused. Close and remove it.
				# self.client.setsockopt(zmq.LINGER, 0)
				# self.client.close()

				# if retries_left >= 0:
				# 	# logging.error("Server seems to be offline, abandoning")
				# 	# sys.exit()

				# 	print('RESENDING', retries_left)

				# 	reply[0] = TASK_REQUEST_SIGNAL
				# 	self.client.send_multipart(reply)

					



				# logging.info("Reconnecting to server…")
				# # Create new connection
				# self.client = self.context.socket(zmq.REQ)
				# self.client.connect(SERVER_ENDPOINT)
				# logging.info("Resending (%s)", request)
				# self.client.send_multipart(request)

			self.bar.report(len(self.results))

		if len(self.results) == len(self._task_keys):

			self.bar.completion_report()

		self.close()
		
		time.sleep(0.01)

		if self.mode=='normal':

			return list(self.results.values())

		# elif self.mode=='pandas':

		# 	if self.sub_mode=='nonpartition':

		# 		for k, v in self.results.items():

		# 			if (isinstance(v, np.ndarray)) & (len(v)==len(self.dataframe)) & (not isinstance(k, int)):
					
		# 				self.dataframe[k] = v

		# 	return self.dataframe


