from objects import Log, TimeFormatter, Heap_Manager
from collections import OrderedDict
from datetime import timedelta

#MACROS
TOP_N_HOURS = 10

def orginal_format_helper(dt):
	months = {1: "Jan", 2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun", 7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"}
	original_format = ""
	day = dt.day
	if day < 10: day = "0"+str(day)
	month = months[dt.month]
	year = dt.year
	time = dt.time()
	original_format += str(day)+"/"+str(month)+"/"+str(year)+":"+str(time)+" -0400"
	return original_format

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
	###Populate OrderedDict with seconds
	f = open(data)
	first = None
	last = None
	total_count = OrderedDict() 
	for i in f:
		log = Log(i)
		dt = TimeFormatter(log.time).dt
		if first == None:
			first = dt
		last = dt
	f.close()
	while first < last:
		total_count[first] = 0
		first = first + timedelta(seconds=1)
	###

	###Count visits for every datetime
	f = open(data)
	for i in f:
		log = Log(i)
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
			f.write(orginal_format_helper(v[1])+','+ str(v[0]) + '\n')
		else:
			f.write(orginal_format_helper(v[1])+','+str(v[0]))
	f.close()
	###	

if __name__ == '__main__':
	busiest_hours('./log_input/log.txt', './log_output/hours.txt')
