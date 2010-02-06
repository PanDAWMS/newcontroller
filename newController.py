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

import os, sys, commands

from controllerSettings import *
from miscUtils import *
from dbAccess import *
from dictHandling import *
from Integrators import *
from configFileHandling import *
from jdlController import *

def loadJdl():
	'''Runs the jdllist table updates'''

	jdldb = {}
	jdlListAdder(jdldb)
	jdldc=buildJdlDict()

	if genDebug: return jdldb, jdldc
	return 0

def loadConfigs():
	'''Run the schedconfig table updates'''

	# Load the database as it stands as a primary reference
	dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())

	# Load the present config files
	configd = buildDict()

	# Load the JDL

	jdldb, jdldc = loadJdl()

	# Add the BDII information
	bdiiIntegrator(configd, dbd)

	# Now add ToA information
	toaIntegrator(configd)
	
	# Compose the "All" queues for each site
	status = allMaker(configd)

	# Compare the DB to the present built configuration
	up_d, del_d = compareQueues(collapseDict(dbd), collapseDict(configd))
	jdl_up_d, jdl_del_d = compareQueues(jdldb, jdldc)
	
	del_l = buildDeleteList(del_d,'schedconfig')
	up_l = buildUpdateList(up_d,param)
	jdl_l = buildUpdateList(jdl_up_d,jdl)

	if safety is not 'on':
		utils.initDB()
		for i in del_l:
			utils.dictcursor().execute(i)

		print 'Updating SchedConfig'
		utils.replaceDB('schedconfig',up_l,key='nickname')
		print 'Updating JDLList'
		utils.replaceDB('jdllist',jdl_l,key='name')
		utils.commit()
		
	newdb, sk = sqlDictUnpacker(loadSchedConfig())

	checkUp, checkDel = compareQueues(collapseDict(newdb), collapseDict(dbd))
	if genDebug: return dbd, configd, up_d, del_d, del_l, up_l, jdl_l, jdldc, newdb, checkUp, checkDel
	return 0
	

if __name__ == "__main__":
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
	dbd, configd, up_d, del_d, del_l, up_l, jdl_l, newjdl, newdb, checkUp, checkDel = loadConfigs()

	os.chdir(base_path)
