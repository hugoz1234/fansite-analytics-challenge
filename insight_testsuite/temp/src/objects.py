from heapq import heappush, heappop, heappushpop, heapify

class Log(object):
	"""Neatly formated and accessable Log"""
	def __init__(self, raw_line):
		"""Log variables kept as string rep for simplicity"""
		aux = raw_line.split(" ")
		bytes = 0 if aux[-1] == "-\n" else aux[-1]

		self.host = aux[0]
		self.timestamp = aux[3]+aux[4]
		self.http_req = raw_line.split("\"")[1]
		self.reply = aux[-2]
		self.bytes = int(bytes.rstrip()) if type(bytes) == type('') else bytes
		#helpful values
		self.success = False if (self.reply[0] == '5' or self.reply[0] == '4') else True
		self.isGet = True if self.http_req.split(" ")[0] == "GET" else False
		self.resource = self.http_req.split(" ")[1] if self.success else None

	def __str__(self):
		return self.host + '**' + self.timestamp + '**' + self.http_req + '**' + self.reply + '**' + str(self.bytes)

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
