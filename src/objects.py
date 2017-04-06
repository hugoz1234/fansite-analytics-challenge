from heapq import heappush, heappop, heappushpop, heapify
from datetime import datetime, timedelta
from collections import OrderedDict

#MACROS
FIVE_MINUTES = 300
PENALTY_TIME = 20

class Log(object):
	"""Neatly formated and accessable Log"""
	def __init__(self, raw_line):
		"""Log variables kept as string rep for simplicity"""
		aux = raw_line.split(" ")
		bytes = 0 if aux[-1] == "-\n" else aux[-1]

		self.host = aux[0]
		self.time = aux[3]+aux[4]
		self.http_req = raw_line.split("\"")[1]
		self.reply = aux[-2]
		self.bytes = int(bytes.rstrip()) if type(bytes) == type('') else bytes
		#helpful values
		self.success = False if (self.reply[0] == '5' or self.reply[0] == '4') else True
		self.isGet = True if self.http_req.split(" ")[0] == "GET" else False
		self.resource = self.http_req.split(" ")[1] if self.success else None
		if not self.isGet and self.resource == None:
			try:
				self.resource = self.http_req.split(" ")[1]
			except:
				pass

	def __str__(self):
		return self.host + '**' + self.time + '**' + self.http_req + '**' + self.reply + '**' + str(self.bytes)

class Heap_Manager(object):
	""" Responsible for storing the largest max_n of a stream of elements. Does not store more values than max_n.

		elements in heap are tuples with the following structure: (count, name)
			- where 'count' is an int and represents the count of the element
			- where 'name' is a string and represents the name of the element
	"""
	def __init__(self, max_n):
		self.heap = []
		self.max = max_n

	def process(self, element):
		if len(self.heap) < self.max:
			heappush(self.heap, element)
		else:
			if element[0] > self.get_min_count():
				heappushpop(self.heap, element)	

	def get_min_count(self):
		if len(self.heap) == 0: return 0
		return self.heap[0][0]

class TimeFormatter(object):
	"""Reformats timestamps to be more useful"""
	def __init__(self, raw):
		aux = raw[1:len(raw)-1].split("/")
		auxx = raw[1:len(raw)-1].split(":")
		auxxx = auxx[3].split("-")

		self.day = aux[0]
		self.month = aux[1]
		self.year = aux[2].split(":")[0]
		self.hour = auxx[1]
		self.minute = auxx[2]
		self.second = auxxx[0][:2]
		self.timezone = auxxx[1]

		complete_time_string = self.day+"-"+self.month+"-"+self.year+" "+self.hour+":"+self.minute+":"+self.second
		format_string = "%d-%b-%Y %H:%M:%S"
		self.dt = datetime.strptime(complete_time_string, format_string)
#####FEATURE 4

class TimeOut(object):
	"""Defines an interval of time during which certain addresses should have been blocked"""
	def __init__(self, host, start, end):
		self.host = host
		self.start = start
		self.end = end

class ThreeStrikesYoureOut(object):
	"""Keeps track of how many offenses an ip has committed. Keeps ips in three categories: those who have
		committed one and two failed attempts, and upon a third failed attempt within the 20 second window
		will keep track of a TimeOut object for the ip. 
	"""
	def __init__(self):
		self.strike_one = {}
		self.strike_two = {}
		self.timeouts = {} #values are TimeOut objects

	def process(self, host, dt, success):
		"""Daemon to other process handlers"""
		if success:
			return self.process_success(host, dt)
		return self.process_failure(host, dt)

	def process_failure(self, host, dt):
		"""Handles a failed login attempts. Returns true to indicate that log should be blocked"""
		if host in self.timeouts:
			if dt < self.timeouts[host].end:
				return True
			else:
				del self.timeouts[host]
				self.strike_one[host] = dt
		elif host in self.strike_two:
			diff = self.get_time_difference(self.strike_two[host], dt)

			if diff < PENALTY_TIME:
				time_out_ending = dt + timedelta(0, FIVE_MINUTES)
				self.timeouts[host] = TimeOut(host, dt, time_out_ending)
			else:
				self.strike_one[host] = dt

			del self.strike_two[host]
		elif host in self.strike_one:
			diff = self.get_time_difference(self.strike_one[host], dt)
			if diff < PENALTY_TIME:
				self.strike_two[host] = self.strike_one[host]
				del self.strike_one[host]
			else:
				self.strike_one[host] = dt
		else:
			self.strike_one[host] = dt
		return False

	def process_success(self, host, dt):
		"""Handles successful requests. Returns True to indicate a log should have been block since 
		it occured during a blocked interval. Clears ips that had a successful attempt and are in the strike_one
		or strike_two hashes.
		"""
		if host in self.timeouts:
			if dt < self.timeouts[host].end:
				return True
			else:
				del self.timeouts[host]
		elif host in self.strike_two or host in self.strike_one:
			self.strike_two.pop(host, None)
			self.strike_one.pop(host, None)
		return False

	def get_time_difference(self, dt1, dt2):
		"""gets the time difference between two datetimes in seconds
			** dt2 occurs later in time than dt1 
		"""
		delta = dt2 - dt1
		return delta.seconds