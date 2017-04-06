from objects import Log, TimeFormatter, Heap_Manager
from collections import OrderedDict
from datetime import timedelta

#MACROS
TOP_N_HOURS = 10

def get_visits_for_hour(start_dt, total_count):
	total = total_count[start_dt]
	for i in range(1,60):
		delta = start_dt + timedelta(seconds=i)
		if delta in total_count:
			total += total_count[delta]
	return total

def busiest_hours(data, outputfile):
	"""Calculates top 10 hours of activity. Only considers successful responses. All time is assumed to be in the same timezone.

		Approach: total_count holds a python datetime as a key, values are the number of successful access that occured at that time.
		On the first pass we count how many access at each unique datetime. Then on the second pass we can ahead an hour to see how many
		accesses occured in that hour.
		Uses min-heap to keep track of top 10.
	"""
	###Count visits for every datetime
	f = open(data)
	total_count = OrderedDict() 
	for i in f:
		log = Log(i)
		if log.success:
			dt = TimeFormatter(log.time).dt
			if dt in total_count:
				total_count[dt] += 1
			else:
				total_count[dt] = 1
	f.close()
	###

	###Get top N busiest hours
	heap_m = Heap_Manager(TOP_N_HOURS)
	for dt in total_count:
		element = (get_visits_for_hour(dt, total_count), dt)
		heap_m.process(element)
	top = sorted(heap_m.heap, key=lambda tup: tup[0], reverse=True)
	###

	###Write
	f = open(outputfile, 'w')
	for i, v in enumerate(top):
		if i != TOP_N_HOURS-1:
			f.write(str(v[1])+','+ str(v[0]) + '\n')
		else:
			f.write(str(v[1])+','+str(v[0]))
	f.close()
	###	

if __name__ == '__main__':
	busiest_hours('./log_input/log.txt', './log_output/hours.txt')
