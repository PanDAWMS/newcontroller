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
	unicodeConvert(jdldb)
	unicodeConvert(jdldc)

	return jdldb, jdldc

def loadConfigs():
	'''Run the schedconfig table updates'''
	# Load the database as it stands as a primary reference
	dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())
	# If the DB is overriding the config files, we need to recreate them now.
	if dbOverride:
		# Get the config dictionary directly from teh DB, and process the config file update from it.
		configd, standardkeys = sqlDictUnpacker(loadSchedConfig())
		# Compose the "All" queues for each site
		status = allMaker(configd)
		# Make the necessary changes to the configuration files:
		makeConfigs(configd)
		# Check the changes just committed into Subversion
		svnCheckin('Updated from DB')

	else:
		# Update the local configuration files from SVN
		svnUpdate()
	
	# Load the present config files, based on the SVN update
	configd = buildDict()

	# Load the JDL from the DB and from the config files, respectively
	jdldb, jdldc = loadJdl()

	# Add the BDII information, and build a list of releases
## 	old_rel_db = loadInstalledSW()
 	new_rel_db = {}
 	bdiiIntegrator(configd, new_rel_db, dbd)

	# Check the old DB for releases to delete, and the new one for releases to insert.
## 	delete_sw = [old_rel_db[i] for i in old_rel_db if i not in new_rel_db]
## 	insert_sw = [new_rel_db[i] for i in new_rel_db if i not in old_rel_db]

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

	# If the safety is off, the DB update can continue
	if safety is not 'on':
		utils.initDB()
		# Individual SQL statements to delete queues that need deleted
		for i in del_l:
			try:
				status = utils.dictcursor().execute(i)
			except:
				print 'Failed SQL Statement: ', i
				print status
				print sys.exc_info()[0]

		# Schedconfig table gets updated all at once
		print 'Updating SchedConfig'

		# Since all inputs are unicode converted, all outputs need to be encoded.
		unicodeEncode(up_l)
		utils.replaceDB('schedconfig',up_l,key=dbkey)

		# Jdllist table gets updated all at once
		print 'Updating JDLList'

		# Since all inputs are unicode converted, all outputs need to be encoded.
		unicodeEncode(jdl_l)
		utils.replaceDB('jdllist',jdl_l,key=jdlkey)

		# Changes committed after all is successful, to avoid partial updates
		utils.commit()
		utils.endDB()
		# FIX This string will eventually be filled with changed queue names and other info for the subversion checkin
		svnstring=''

		# Delete outdated installedsw entries
## 		for i in delete_sw:
## 			try:
## 				sql = "DELETE FROM installedsw WHERE release = '%s' and cache = '%s' and siteid = '%s'" % (i['release'],i['cache'],i['siteid'])
## 				utils.dictcursor().execute(sql)
## 			except:
## 				print 'Failed SQL Statement: ', sql, i

## 		# Insert new installedsw entries. Not using replaceDB because of a lack of single DB key.
## 		for i in insert_sw:
## 			try:
## 				sql = "INSERT INTO installedsw (release, cache, siteid, cloud) values ('%s','%s','%s','%s')" % (i['release'],i['cache'],i['siteid'],i['cloud'])
## 				utils.dictcursor().execute(sql)
## 			except:
## 				print 'Failed SQL Statement: ', sql, i
## 		# Commit installedsw changes
## 		utils.commit()




		
	# Check out the db as a new dictionary
	newdb, sk = sqlDictUnpacker(loadSchedConfig())
	
	# If the checks pass (no difference between the DB and the new configuration)
	checkUp, checkDel = compareQueues(collapseDict(newdb), collapseDict(configd))
	if not len(checkUp) + len(checkDel):
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
	if '--dbOverride' in args:
		print 'The DB will override existing config file settings.'
		dbOverride = True
	if '--debug' in args: genDebug = True
	keydict={}

	# All of the passed dictionaries will be eliminated at the end of debugging. Necessary for now.
	dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())

	if genDebug: dbd, configd, up_d, del_d, del_l, up_l, jdl_l, newjdl, newdb, checkUp, checkDel = loadConfigs()
	else: loadConfigs()

	#os.chdir(base_path)
