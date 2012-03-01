

import os, sys, time
from controllerSettings import *

runLogPath = '/afs/cern.ch/user/a/atlpan/scratch0/schedconfig/logs/'
fileStr = '_schedconfigUpdate.txt'

# Check the files in the logging directory
logList = os.listdir(runLogPath)
# Get the uncompressed ones
logList = [i for i in logList if fileStr in i and len(i) < 30]
for log in logList:
	# Change their names to reflect their timestamp
	# Keep the UTC
	atime = os.path.getmtime(runLogPath + log)
	# Create the time text
	date = '_'.join([str(i).zfill(2) for i in tuple(time.localtime(atime))[:6]])
	# Assign the time text to the filename
	os.system('mv %s %s'% (runLogPath + log, runLogPath + date + fileStr))
	# Fix the system timestamp
	os.utime(runLogPath + date + fileStr,(atime,atime))
	# Zip the file
	os.system('gzip %s' % (runLogPath + date + fileStr))
	# Fix the system timestamp
	os.utime(runLogPath + date + fileStr + '.gz',(atime,atime))
	
