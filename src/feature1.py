from objects import Log, Heap_Manager
#Macros
TOP_N_HOSTS = 10

def most_active_hosts(data, outputfile):
	""" Takes log file as input and computes desired number of top hosts with most successful accesses
		filename : name of file (string)

		Approach: min-heap

	"""

	f = open(data)
	total_count = {}
	heap = []
	###Count hosts
	for i in f:				
		log = Log(i)
		if log.host in total_count: 
			total_count[log.host] += 1
		else:
			total_count[log.host] = 1 
	f.close()
	###
	heap_m = Heap_Manager(TOP_N_HOSTS)
	for host in total_count:
		element = (total_count[host], host)
		heap_m.process(element)
	most_active = sorted(heap_m.heap, key=lambda tup: tup[0], reverse=True)

	###Write to file
	f = open(outputfile, 'w')
	for i,v in enumerate(most_active):
		if i != TOP_N_HOSTS-1:
			f.write(str(v[1]) + ',' + str(v[0]) + '\n')
		else:
			f.write(str(v[1]) + ',' + str(v[0]))
	f.close()
	####

	#print "DONE"
		

if __name__ == '__main__':
	most_active_hosts('./log_input/log.txt', './log_output/hosts.txt')