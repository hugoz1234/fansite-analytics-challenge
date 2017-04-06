from objects import Log
#Macros
TOP_N_HOSTS = 10

def most_active_hosts(data, outputfile):
	""" Takes log file as input and computes desired number of top hosts with most successful accesses
		filename : name of file (string)

		Approach: Keeps track of total count of all successful access of unique hosts. Buckets hosts by
		number of accesses. Takes the largest 10 hosts. 

	"""

	f = open(data)
	total_count = {}
	max_count = ('host', 0)

	###Count hosts
	for i in f:				
		log = Log(i)
		if log.success:
			if log.host in total_count: 
				total_count[log.host] += 1
				if total_count[log.host] > max_count[1]:
					max_count = (log.host, total_count[log.host])
			else:
				total_count[log.host] = 1 
	f.close()
	###

	###Create and place into buckets
	buckets = [[]]*(max_count[1]+1)
	for host in total_count:
		count = total_count[host]
		buckets[count].append(host)
	###

	###Extract top n
	most_active = []
	for bucket in range(len(buckets)-1, 0, -1):
		for host in buckets[bucket]:
			most_active.append((host, bucket))
			if len(most_active) == TOP_N_HOSTS:
				break
		if len(most_active) == TOP_N_HOSTS:
			break
	###

	###Write to file
	most_active = sorted(most_active, key=lambda tup: tup[1], reverse=True)
	f = open(outputfile, 'w')
	for i,v in enumerate(most_active):
		if i != TOP_N_HOSTS-1:
			f.write(v[0] + ',' + str(v[1]) + '\n')
		else:
			f.write(v[0] + ',' + str(v[1]))
	f.close()
	####

	#print "DONE"
		

if __name__ == '__main__':
	most_active_hosts('./log_input/log.txt', './log_output/hosts.txt')