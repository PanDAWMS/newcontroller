#! /usr/bin/env python
#######################################################
# Handles storage and modification of queue data      #
# in the ATLAS PanDA schedconfig and jdllist tables   #
# for ATLAS production and analysis queues.           #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 23 Jun 09 #
#######################################################

# TODO:

# Add installedSW table support
# Add a consistency checker
# Add manual pickle restoration code
# Add subversion update, file add and checkin. Comment changes.
# Add change detection to avoid DB change collisions
# Add queue insertion scripts
# Add checking of queue "on" and "off"

# This code has been organized for easy transition into a class structure.

import os, sys, commands, pickle

from controllerSettings import *
from miscUtils import *
from dbAccess import *
from dictHandling import *
from Integrators import *
from configFileHandling import *
from jdlController import *
from svnHandling import *
from backupHandling import *

def loadJdl():
	'''Runs the jdllist table updates'''

	jdldb = {}
	jdlListAdder(jdldb)
	jdldc=buildJdlDict()

	return jdldb, jdldc

def loadConfigs():
	'''Run the schedconfig table updates'''

	# Load the database as it stands as a primary reference
	dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())

	# Update the local configuration files from SVN
	svnUpdate()
	
	# Load the present config files, based on the SVN update
	configd = buildDict()

	# Load the JDL from the DB and from the config files, respectively
	jdldb, jdldc = loadJdl()

	# Add the BDII information
	bdiiIntegrator(configd, dbd)

	# Now add ToA information
	toaIntegrator(configd)
	
	# Compose the "All" queues for each site
	status = allMaker(configd)

	# Compare the DB to the present built configuration to find the queues that are changed.
	up_d, del_d = compareQueues(collapseDict(dbd), collapseDict(configd), dbOverride)
	jdl_up_d, jdl_del_d = compareQueues(jdldb, jdldc, dbOverride)
	deletes = [del_d[i][dbkey] for i in del_d]

	# Delete queues that are not Enabled
	del_d.update(disabledQueues(configd))

	# Get the database updates prepared for insertion.
	# The Delete list is just a list of SQL commands (don't add semicolons!)
	del_l = buildDeleteList(del_d,'schedconfig')

	# The other updates are done using the standard replaceDB method from the SchedulerUtils package.
	# The structure of the list is a list of dictionaries containing column/value as the key/value pairs.
	# The primary key is specified for the replaceDB method. For schedconfig, it's dbkey, and for the jdls it's jdlkey
	# (specified in controllerSettings
	up_l = buildUpdateList(up_d,param)
	jdl_l = buildUpdateList(jdl_up_d,jdl)

	supdates = [i[dbkey] for i in up_l]
	jupdates = [i[jdlkey] for i in jdl_l]

	# If the safety is off, the DB update can continue
	if safety is not 'on':
		utils.initDB()
		# Individual SQL statements to delete queues that need deleted
		for i in del_l:
			try:
				utils.dictcursor().execute(i)
				utils.commit()
			except:
				print 'Failed SQL Statement: ', i
			
		# Schedconfig table gets updated all at once
		print 'Updating SchedConfig'
		utils.replaceDB('schedconfig',up_l,key=dbkey)
		# Jdllist table gets updated all at once
		print 'Updating JDLList'
		utils.replaceDB('jdllist',jdl_l,key=jdlkey)
		# Changes committed after all is successful, to avoid partial updates
		utils.commit()
		# This string will eventually be filled with changed queue names and other info for the subversion checkin
		svnstring=''
		
	# Check out the db as a new dictionary
	newdb, sk = sqlDictUnpacker(loadSchedConfig())
	
	# If the checks pass (no difference between the DB and the new configuration)
	checkUp, checkDel = compareQueues(collapseDict(newdb), collapseDict(configd))
	If not len(checkUp) + len(checkDel):
		print 'Passed checks!'
		# Make the necessary changes to the configuration files:
		makeConfigs(configd)
		# Check the changes just committed into Subversion
		svnCheckin(svnstring)
		# Create a backup pickle of the finalized DB as it stands.
		backupCreate(newdb)
	else: print '########### Differences in the DB/Configs! ###########\n'
	# For development purposes, we can get all the important variables out of the function. Usually off.
	if genDebug: return dbd, configd, up_d, del_d, del_l, up_l, jdl_l, jdldc, newdb, checkUp, checkDel
	return 0
	

if __name__ == "__main__":
	args = sys.argv[1:]
	# A better argument parser will be needed in the future
	if 'dbOverride' in args: dbOverride = True
	keydict={}
	def testDiff(m,n):
		for i in m:
			if type(m[i]) == dict: mm = collapseDict(m)
			else: mm = m
		for i in n:
			if type(n[i]) == dict: nn = collapseDict(n)
			else: nn = n
				
		for i in mm:
			try:
				for k in mm[i].keys():
					if k not in ['jdladd','releases']:
						if mm[i][k] != nn[i][k]:
							print i, k, mm[i][k], nn[i][k], type(mm[i][k]), type(nn[i][k])
						
			except KeyError:
				pass

	# All of the passed dictionaries will be eliminated at the end of debugging. Necessary for now.
	if genDebug: dbd, configd, up_d, del_d, del_l, up_l, jdl_l, newjdl, newdb, checkUp, checkDel = loadConfigs()
	else: loadConfigs()

	os.chdir(base_path)
