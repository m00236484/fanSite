import sys
import os 
import re
from datetime import datetime, date, timedelta
from collections import Counter
from collections import OrderedDict
from operator import itemgetter

class logProcess:
  def __init__(self, inFile, hostPath, hoursPath, resourcePath , blockedPath):
    self.dicHour = {}
    self.dicHost = {}
    self.dicResource = {}
    self.dicResourceMax = {}
    self.dicBlocked = {}
    self.readFile(inFile)
    self.outHours = hoursPath
    self.outHost = hostPath
    self.outResource = resourcePath
    self.outBlocked = blockedPath
    self.genOutput()
    
    
  def readFile(self,inputFile):
    match_record = re.compile(r"^[^ ]+ - (C[^ ]*) \[([^ ]+)").match
    file = open(inputFile, "rb")
      
    i = 0 
    parts = [
      r'(?P<host>\S+)',                   # host %h
      r'\S+',                             # indent %l (unused)
      r'(?P<user>\S+)',                   # user %u
      r'\[(?P<time>.+)\]',                # time %t
      r'"(?P<request>.*)"',               # request "%r"
      r'(?P<status>[0-9]+)',              # status %>s
      r'(?P<size>\S+)',                   # size %b (careful, can be '-')
        ]
    pattern = re.compile(r'\s+'.join(parts)+r'\s*\Z') 
    # Regex for a feed request.
    feed = re.compile(r'/all-this/(\d\d\d\d/\d\d/[^/]+/)?feed/(atom/)?')
    dicAna = {}
    dicval = []

    for line in file:
      m  = pattern.match(line)
      hit = m.groupdict()
      cons = 0	
      dicval = []	
      if hit != None:
        host = hit["host"]
        Scons = hit["size"]
        request = hit["request"]
        time = hit["time"]
        ttime = hit["time"]
        time = time[0:15]+"00:00"+time[20:]
        status = hit["status"]
        if Scons == "-":
           cons = 0 
        else:
           cons = int(Scons)
                   
        if dicAna.has_key(host):
          dicval = dicAna.get(host)
          dicval[0] += 1
          dicval[1] += cons 		
        else:
          dicval.append(1)	  
          dicval.append(cons)
        dicAna[host]= dicval   	      
      self.hostsAccess(host , dicval[0] )
      self.resourceConsume(request , cons )
      self.hourVisits(time )
      self.hostsBlocked(host , ttime , status)

  def hostsAccess(self, host, accessNo):
    lKey = ""
    pKey = ""
    stop = True
    if self.dicHost.has_key(host):
      self.dicHost[host] = accessNo
    else:
      self.dicHost[host] = accessNo
    d = OrderedDict(sorted(self.dicHost.items(), key=itemgetter(1), reverse=True))
    #print d
    self.dicHost.clear()
    while len(d) > 10: #and stop:
      lKey = d.keys()[-1]
      del d[lKey]
      '''
      pKey = d.keys()[-2]  
      if d[lKey] < d[pKey]:
        print 1	
        del d[lKey]
      else:
        stop = False
      '''
    self.dicHost = d

  def resourceConsume(self,request, consume):
    lKey = ""
    pKey = ""
    stop = True
    url =  request[4:]
    if self.dicResource.has_key(url):
      self.dicResource[url] += consume
      consume = self.dicResource[url]
    else:
      self.dicResource[url] = consume

    self.dicResourceMax[url] = consume
    d = OrderedDict(sorted(self.dicResourceMax.items(), key=itemgetter(1), reverse=True))
    #print d
    while len(d) > 10: #and stop:
      lKey = d.keys()[-1]
      del d[lKey]
      '''
      pKey = d.keys()[-2]  
      if d[lKey] < d[pKey]:
        print 1	
        del d[lKey]
      else:
        stop = False
      '''
    self.dicResourceMax.clear()  
    self.dicResourceMax = d

  def hourVisits(self, hour):
    lKey = ""
    pKey = ""
    stop = True
    if self.dicHour.has_key(hour):
      self.dicHour[hour] += 1
    else:
      self.dicHour[hour] = 1
    d = OrderedDict(sorted(self.dicHour.items(), key=itemgetter(1), reverse=True))
    #print d
    self.dicHour.clear()
    while len(d) > 10: #and stop:
      lKey = d.keys()[-1]
      del d[lKey]
      '''
      pKey = d.keys()[-2]  
      if d[lKey] < d[pKey]:
        print 1	
        del d[lKey]
      else:
        stop = False
      '''
    self.dicHour = d

  def hostsBlocked(self, host , time , status):
    lKey = ""
    pKey = ""
    stop = True
    secondSeq = []
    
    if self.dicBlocked.has_key(host):
      secondSeq = self.dicBlocked[host]
      if len(secondSeq) == 4 or len(secondSeq) == 2:
        if time - secondSeq[0] > 20:
          del secondSeq[0, 2]
        else:
          secondSeq.appened(time)
          secondSeq.appened(1)
    elif not self.dicBlocked.has_key(host) and status == "401":
      secondSeq = [time, 0]
      #self.dicBlocked[hour] = 1
    else:
      secondSeq = [time, 0]
      #self.dicBlocked[hour] = 1
    d = OrderedDict(sorted(self.dicBlocked.items(), key=itemgetter(1), reverse=True))
    #print d
    self.dicBlocked.clear()
    while len(d) > 10: #and stop:
      lKey = d.keys()[-1]
      del d[lKey]
      '''
      pKey = d.keys()[-2]  
      if d[lKey] < d[pKey]:
        print 1	
        del d[lKey]
      else:
        stop = False
      '''
    self.dicBlocked = d

  
  def genOutput(self):
    self.write_report(self.dicResourceMax ,self.outResource)
    self.write_report(self.dicHost , self.outHost)
    self.write_report(self.dicHour , self.outHours )
    self.write_report(self.dicBlocked , self.outBlocked)
    

  def writeOutput(dic, fileName, outFolder):
    return 1
    
      
    
  def write_report(self, dict, filename):
    input_filename=open(filename, "w")
    outString =""
    for (k,v) in dict.items():
      outString = ""
      outString = k+","+str(v)+"\n"
      input_filename.write(outString)
    input_filename.close()  
    return filename
        
def main():
  if len(sys.argv) >= 5:
    inFile = sys.argv[1]
    outHost = sys.argv[2]
    outHours = sys.argv[3]
    outResource = sys.argv[4]
    outBlocked = sys.argv[5]
    lp = logProcess(inFile, outHost, outHours, outResource , outBlocked)
  else:
    print 'Please Enter Input and Output Files '

  
  '''
  for file in files:
    #inpuFile = inputDir + "\" + file
    inpuFile = "D:\\Projects\\fanSite\\log_input\\log.txt" 	
    readFile(inpuFile)  
    print("Done!")  
  '''
  #readFile()
# This is the standard boilerplate that calls the main() function.
if __name__ == '__main__':
    main()  
