#! /usr/bin/env python
#######################################################
# Restores volatile schedconfig data in a pinch       #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 18 Oct 11 #
#######################################################

import os, sys, commands, gzip
from SchedulerUtils import utils

# Load the gzip file
try:
	inputFile = sys.argv[1]
	f=gzip.open(inputFile)
except:
	print 'Input file not specified or not available'
	sys.exit()

# Pay attention to the safeties
if os.environ.has_key('DBINTR'): setINTR = True
else: setINTR = False

# Initialize DB
utils.dbname='pmeta'
if safety is 'on': utils.setTestDB()
if setINTR:
	utils.setTestDB()
	print 'Using INTR Database'
utils.initDB()
print "Init DB"

# Restore status line-by-line
for i in f:
	sql = i
	nrows = utils.dictcursor().execute(sql)

f.close()

# Commit the changes
utils.commit()
utils.endDB()

	
