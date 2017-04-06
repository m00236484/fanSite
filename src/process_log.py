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
    self.dicBlocked = []
    self.dicblockedLog ={}
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
        #ttime = hit["time"]
        time = time[0:15]+"00:00"+time[20:]
        status = int(hit["status"])
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
      ttime, zone = hit["time"].split()
      hit["time"] = datetime.strptime(ttime , "%d/%b/%Y:%H:%M:%S")
      
      self.hostsAccess(host , dicval[0] )
      self.resourceConsume(request , cons )
      self.hourVisits(time )
      self.hostsBlocked(host , ttime , status , line)
      if i == 100000: return
      i = i+1

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
    self.dicHour = d

  def hostsBlocked(self, host , time , status , hit):
    
    #print(int(datetime.strptime(time , "%d/%b/%Y:%H:%M:%S")))
    blockedLog=[] #[0 firstTime, 1: no of failuer, 2: blocked ,3: starttimeto block]
    if self.dicblockedLog.has_key(host):
      blockedLog = self.dicblockedLog[host]
      prvTime = datetime.strptime(blockedLog[0] , "%d/%b/%Y:%H:%M:%S")
      currentTime = datetime.strptime(time , "%d/%b/%Y:%H:%M:%S")
      blcokedTime = datetime.strptime(blockedLog[3] , "%d/%b/%Y:%H:%M:%S")
      #print(prvTime, currentTime ,abs(prvTime  - currentTime))
      if blockedLog[2] >= 1 and (abs(currentTime - blcokedTime ) <= timedelta(minutes=5)): # in blocked period
        # add to blocked record
        self.dicBlocked.append(hit)
        
      elif blockedLog[1]>=1 and  status == 401 and (abs(prvTime  - currentTime) <= timedelta(seconds=20)):
        # change to block log and add to blog log
        if blockedLog[1] == 1:
          blockedLog[1] += 1
          self.dicblockedLog[host] = blockedLog
          #increment failuer attempet
        else:
          self.dicBlocked.append(hit)
          blockedLog[2] += 1
          self.dicblockedLog[host] = blockedLog
          # change to block log and add to blog log
      elif status == 401:
        blockedLog.clear()
        #add new block check
        blockedLog.append(time)
        blockedLog.append(1)
        blockedLog.append(0)
        blockedLog.append(time)
        self.dicblockedLog[host] = blockedLog
      else:
        # delete from block check
        del self.dicblockedLog[host]
      
    else:
      if status == 401:
        blockedLog.append(time)
        blockedLog.append(1)
        blockedLog.append(0)
        blockedLog.append(time)
        self.dicblockedLog[host] = blockedLog
        
      #self.dicBlocked[hour] = 1
      
  def genOutput(self):
    self.write_report(self.dicResourceMax,2 ,self.outResource)
    self.write_report(self.dicHost ,0, self.outHost)
    self.write_report(self.dicHour , 0, self.outHours )
    self.write_report(self.dicBlocked , 1, self.outBlocked)
    

  def writeOutput(dic, fileName, outFolder):
    return 1
    
  def write_report(self, dict,i, filename):
    input_filename=open(filename, "w")
    outString =""
    if i == 1 :
      for item in dict:
        outString =str(item) 
        input_filename.write(outString)
    else:
      for (k,v) in dict.items():
        outString = ""
        if  i == 2 :
          outString = k+"\n"
        else:
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
