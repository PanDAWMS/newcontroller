# Maintain the backups well.

import time

from controllerSettings import *

fileStr = '_schedconfigUpdate.txt'

# Check the files in the logging directory
logList = os.listdir(runLogPath)
# Get the uncompressed ones
logList = [i for i in logList if fileStr in i and '.gz' not in i]
for log in logList:
    # Change their names to reflect their timestamp
    # Keep the UTC
    atime = os.path.getmtime(runLogPath + log)
    # Create the time text
    date = '_'.join([str(i).zfill(2) for i in tuple(time.localtime(atime))[:6]])
    # Assign the time text to the filename
    os.system('mv %s %s' % (runLogPath + log, runLogPath + date + fileStr))
    # Fix the system timestamp
    os.utime(runLogPath + date + fileStr, (atime, atime))
    # Zip the file
    os.system('gzip %s' % (runLogPath + date + fileStr))
    # Fix the system timestamp
    os.utime(runLogPath + date + fileStr + '.gz', (atime, atime))

# Check the files in the logging directory
logList = os.listdir(runLogPath)
# Get the uncompressed ones, sort them
logList = sorted([i for i in logList if fileStr + '.gz' in i])
for i in logList[:-keptRunLogs]:
    # And remove the expired ones
    os.remove(runLogPath + i)
