#! /usr/bin/env python
#######################################################
# Handles storage and modification of queue data      #
# in the ATLAS PanDA schedconfig and jdllist tables   #
# for ATLAS production and analysis queues.           #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 23 Jun 09 #
#######################################################

# TODO:

# Add manual pickle restoration code

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
from swHandling import *
from lesserTablesController import *
from accessControl import *
from svnConsistencyChecker import *

try:
	import lcgInfositeTool2 as lcgInfositeTool
except:
	print "Cannot import lcgInfositeTool, will exit"
	sys.exit(-1)


def loadConfigs():
	'''Run the schedconfig table updates'''
	# Load the database as it stands as a primary reference
	dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())
	# If the DB is overriding the config files, we need to recreate them now.
	if dbOverride:
		# Get the config dictionary directly from the DB, and process the config file update from it.
		configd, standardkeys = sqlDictUnpacker(loadSchedConfig())
		# Compose the "All" queues for each site
		status = allMaker(configd, initial=True)
		# Make the necessary changes to the configuration files:
		makeConfigs(configd)
		# Check the changes just committed into Subversion
		svnCheckin('Updated from DB')
		
	else:
		# Update the local configuration files from SVN after checking to be sure there
		# are no inconsistencies.
		if checkConfigs() > -1:
			svnUpdate()
		else:
			emailError('Persistent inconsistencies in config SVN')
			sys.exit()
		
	
	# Load the present config files, based on the SVN update
	configd = buildDict()
	
	# Load the JDL from the DB and from the config files, respectively
	jdldb, jdldc = loadJdl()
	# Add the BDII information, and build a list of releases
	if not bdiiOverride:
		linfotool = lcgInfositeTool.lcgInfositeTool()
	
	# Now add ToA information
	if not toaOverride: toaIntegrator(configd)
	# and integrate the BDII information
	#if not bdiiOverride: bdiiIntegrator(configd,dbd,linfotool)
	# Compose the "All" queues for each site
	status = allMaker(configd, initial = False)

	# Make sure all nicknames are kosher
	nicknameChecker(configd)
	nicknameChecker(dbd)

	# Compare the DB to the present built configuration to find the queues that are changed.
	up_d, del_d = compareQueues(collapseDict(dbd), collapseDict(configd), dbOverride)
	if delDebug:
		print '******* Compare step'
		print len(del_d)
		print del_d.keys()
		print
	jdl_up_d, jdl_del_d = compareQueues(jdldb, jdldc, dbOverride)
	deletes = [del_d[i][dbkey] for i in del_d]
	if delDebug: print len(deletes)
	
	# Delete queues that are not Enabled and are in the DB
	if delDebug:
		print '******* Disabled queues list'
		#print disabledQueues(dbd,configd).keys()
		print len(disabledQueues(dbd,configd).keys())
	del_d.update(disabledQueues(dbd,configd))
	# Get the database updates prepared for insertion.
	# The Delete list is just a list of SQL commands (don't add semicolons!)
	del_l = buildDeleteList(del_d,'schedconfig')
	# The other updates are done using the standard replaceDB method from the SchedulerUtils package.
	# The structure of the list is a list of dictionaries containing column/value as the key/value pairs.
	# The primary key is specified for the replaceDB method. For schedconfig, it's dbkey, and for the jdls it's jdlkey
	# (specified in controllerSettings
	up_l = buildUpdateList(up_d,param,dbkey)
	jdl_l = buildUpdateList(jdl_up_d,jdl,jdlkey)


	# If the safety is off, the DB update can continue
	if safety is not 'on':
		utils.initDB()
		unicodeEncode(del_l)
		# Individual SQL statements to delete queues that need deleted
		if not delDebug:
			print '\n\n Queues Being Deleted:\n'
			for i in sorted(del_d): print del_d[i][dbkey],del_d[i]['cloud'] 
			
			for i in del_l:
				try:
					status = utils.dictcursor().execute(i)
				except:
					print 'Failed SQL Statement: ', i
					print status
					print sys.exc_info()
		else:
			print "********** Delete step has been DISABLED!"

		# Schedconfig table gets updated all at once
		print 'Updating SchedConfig'

		# Since all inputs are unicode converted, all outputs need to be encoded.
		print '\n\n Queues Being Updated or Added:\n'
		for i in sorted(up_d): print up_d[i][dbkey], up_d[i]['cloud']
		unicodeEncode(up_l)
		status=utils.replaceDB('schedconfig',up_l,key=dbkey)
		status=status.split('<br>')
		if len(status) < len(up_l):
			status=[]
			for up in up_l:
				status.append(utils.replaceDB('schedconfig',[up],key=dbkey))
			errors = [stat for stat in status if 'Error' in stat]
			f=file(errorFile,'w')
			f.write(str(errors))
			f.close()
			shortErrors = [')</b></font>'+err.split(')</b></font>')[1] for err in errors]
			emailError(str(shortErrors))

		# Jdllist table gets updated all at once
		print 'Updating JDLList'

		# Since all inputs are unicode converted, all outputs need to be encoded.
		unicodeEncode(jdl_l)
		status=utils.replaceDB('jdllist',jdl_l,key=jdlkey)
		
		status=status.split('<br>')
		if len(status) < len(jdl_l):
			print "Failed JDL load"
			status=[]
			for up in jdl_l:
				status.append(utils.replaceDB('jdllist',[up],key=jdlkey))
			errors = [stat for stat in status if 'Error' in stat]
			f=file(errorFileJDL,'w')
			f.write(str(errors))
			f.close()
			shortErrors = [')</b></font>'+err.split(')</b></font>')[1] for i in errors]
			emailError(str(shortErrors))
		

		# Changes committed after all is successful, to avoid partial updates
		utils.commit()
		utils.endDB()
		# FIX This string will eventually be filled with changed queue names and other info for the subversion checkin
		svnstring=''
	# If the safety is on:
	else:
		svnstring=''

	# Check out the db as a new dictionary
	newdb, sk = sqlDictUnpacker(loadSchedConfig())
 	if not bdiiOverride:
 		#if genDebug: sw_db, sw_bdii, delList, addList, confd, cloud, siteid, gk=updateInstalledSW(collapseDict(newdb),linfotool)
		if genDebug:
			print 'Received debug info'
			sw_db, sw_bdii, deleteList, addList, confd, cloud, siteid, gatekeeper = updateInstalledSW(collapseDict(newdb),linfotool)			
 		else: updateInstalledSW(collapseDict(newdb),linfotool)
	# If the checks pass (no difference between the DB and the new configuration)
	checkUp, checkDel = compareQueues(collapseDict(newdb), collapseDict(configd))
	
	# Make the necessary changes to the configuration files:
	makeConfigs(configd)
	# Check the changes just committed into Subversion, unless we're not updating from BDII/ToA
	if not toaOverride and not bdiiOverride and safety is 'off': svnCheckin(svnstring)
	# Create a backup pickle of the finalized DB as it stands.
	backupCreate(newdb)

	# Fix any needed sites/clouds on access control
	# readAccessControl()

	# For development purposes, we can get all the important variables out of the function. Usually off.
	if genDebug:
		return sw_db, sw_bdii, deleteList, addList, confd, cloud, siteid, gatekeeper, linfotool, dbd, configd, up_d, del_d, del_l, up_l, jdl_l, jdldb, jdldc, newdb, checkUp, checkDel
	else:
		return 0
	

if __name__ == "__main__":
	args = sys.argv[1:]
	# A better argument parser will be needed in the future
	if '--dbOverride' in args:
		print 'The DB will override existing config file settings.'
		dbOverride = True
	if '--bdiiOverride' in args:
		print 'BDII updating disabled.'
		bdiiOverride = True
	if '--toaOverride' in args:
		print 'ToA updating disabled.'
		toaOverride = True
	if '--safety' in args:
		print 'Safety is ON! No writes to the DB.'
		safety = 'on'
	if '--debug' in args: genDebug = True
	keydict={}

	# Backup of all the volatile DB paramaters before the operation
	volatileBackupCreate()
	if not genDebug:
		try:
			# All of the passed dictionaries will be eliminated at the end of debugging. Necessary for now.
			dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())
			loadConfigs()
		except:
			emailError(sys.exc_value)
	else:
		l=[]
		# All of the passed dictionaries will be eliminated at the end of debugging. Necessary for now.
		dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())
		print 'L is OK'
		sw_db, sw_bdii, deleteList, addList, confd, cloud, siteid, gatekeeper, linfotool, dbd, configd, up_d, del_d, del_l, up_l, jdl_l, jdldb, jdldc, newdb, checkUp, checkDel = loadConfigs()


		
	#os.chdir(base_path)
