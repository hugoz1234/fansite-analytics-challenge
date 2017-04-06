from objects import Log, Heap_Manager
import heapq

#MACROS
TOP_N_RESOURCES = 10

def biggest_resources(data, outputfile):
	""" Takes log file as input and computes desired number of top resources with most consumed bandwith
		filename : name of file (string)

		Approach: the problem seems similar to feature 1 however a different algorithm is required since
		the previous algorithm relied on low count totals since it created an array the size of the largest count.
		I used a min-heap to keep track of the largest 10 elements to avoid large array allocations.

	"""

	f = open(data)
	total_count = {}
	heap = []
	###Count Resource Bandwidth
	for i in f:				
		log = Log(i)
		if log.success:
			if log.resource in total_count: 
				total_count[log.resource] += log.bytes
			else:
				total_count[log.resource] = log.bytes
	return
	f.close()
	###

	###Extract top n with min-heap
	heap_m = Heap_Manager(TOP_N_RESOURCES)
	for resource in total_count:
		element = (total_count[resource], resource)
		heap_m.process(element)
	top = sorted(heap_m.heap, key=lambda tup: tup[0], reverse=True)
	###

	###Write to file
	f = open(outputfile, 'w')
	for i, v in enumerate(top):
		if i != TOP_N_RESOURCES-1:
			f.write(v[1]+'\n')
		else:
			f.write(v[1])
	f.close()
	###

	#print "DONE"

if __name__ == '__main__':
	biggest_resources('./log_input/log.txt', './log_output/resources.txt')