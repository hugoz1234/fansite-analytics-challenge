from objects import Log, TimeFormatter, ThreeStrikesYoureOut

def blocked(data, outputfile):
	"""Determines users who should have been blocked after 3 unsuccessful attempts within 20 seconds"""
	f = open(data)
	out_f = open(outputfile, 'w')
	tracker = ThreeStrikesYoureOut()
	
	for raw_log in f:
		log = Log(raw_log)
		dt = TimeFormatter(log.time).dt
		blocked = tracker.process(log.host, dt, log.success) 
		if blocked:
			out_f.write(raw_log)

	f.close()
	out_f.close()

if __name__ == '__main__':
	blocked('./log_input/log.txt', './log_output/blocked.txt')