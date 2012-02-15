#! /usr/bin/env python
#######################################################
# Restores schedconfig data in a pinchi               #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 15 Feb 12 #
#######################################################

import os, sys, commands, gzip
from SchedulerUtils import utils

safety = 'off'

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
	sql = i.strip(';').replace('atlas_pandameta.schedconfig','schedconfig')
	nrows = utils.dictcursor().execute(sql)

f.close()

# Commit the changes
utils.commit()
utils.endDB()

	
