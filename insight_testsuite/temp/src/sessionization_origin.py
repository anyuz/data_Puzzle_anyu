import csv
import sys
from datetime import datetime

class Record:
	def __init__(self,ip=None,start=None,end=None,request=0,sess=None):
		self.ip=ip
		self.start=start
		self.end=end
		self.request=request
		self.sess=sess
	def output(self):
		st = self.start.strftime("%Y-%m-%d %H:%M:%S")
		ed = self.end.strftime("%Y-%m-%d %H:%M:%S")
		duration = int((self.end - self.start).total_seconds() + 1)
		return "%s,%s,%s,%d,%d\n"%(self.ip,st,ed,duration,self.request)



flog=sys.argv[1]
fint=sys.argv[2]
fout=sys.argv[3]

interval=int(open(fint,"r").read())
iplib=dict()
timelib=dict()

reader = csv.reader(open(flog,"rb"))
reader.next()

result = open(fout,"w")

numsess = 0 # used to determine order
for line in reader:
	# print "---------",count
	# print iplib
	# print timelib

	ip,date,time=line[0],line[1],line[2]
	if '/' in date:
		dt=datetime.strptime(date+time,"%m/%d/%y%H:%M:%S")
	else:
		dt=datetime.strptime(date+time,"%Y-%m-%d%H:%M:%S")
	keys = timelib.keys()
	for timestamp in keys:# sessesion out
		if (dt-timestamp).total_seconds()>interval:
			output = []
			for ipx in timelib[timestamp]:
				record = iplib[ipx]
				output.append((record.start,record.sess,record.output()))
				del iplib[ipx] 
			output.sort()
			result.write(''.join([_[-1] for _ in output]))
				
			del timelib[timestamp]
	

	
	if ip not in iplib:
		iplib[ip] = Record(ip,dt,dt,1,numsess)
		numsess += 1
		if dt not in timelib:
			timelib[dt]=set([ip])
		else:
			timelib[dt].add(ip)
	else:
		timelib[iplib[ip].end].remove(ip)
		if dt not in timelib:
			timelib[dt]=set([ip])
		else:
			timelib[dt].add(ip)
		iplib[ip].end = dt
		iplib[ip].request+=1

output=[]
for ipx in iplib:
	record = iplib[ipx]
	output.append((record.start,record.sess,record.output()))
output.sort()
result.write(''.join([_[-1] for _ in output]))



result.close()