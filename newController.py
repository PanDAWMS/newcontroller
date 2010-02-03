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

	jdld = {}
	jdlListAdder(jdld)
	buildJdlFiles(jdld)
	newjdl=buildJdlDict()

	if genDebug: return jdld, newjdl
	return 0

def loadConfigs():
	'''Run the schedconfig table updates'''

	# Load the database as it stands as a primary reference
	dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())

	# Load the present config files
	configd = buildDict()

	# Load the JDL

	jdld, newjdl = loadJdl()

	# Add the BDII information
	bdiiIntegrator(configd, dbd)

	# Now add ToA information
	toaIntegrator(configd)
	
	# Compose the "All" queues for each site
	status = allMaker(configd)

	# Compare the DB to the present built configuration
	up_d, del_d = compareQueues(collapseDict(dbd), collapseDict(configd))

	del_l = buildDeleteList(del_d,'schedconfig')
	up_l = buildUpdateList(up_d)
	jdl_l = buildUpdateList(newjdl)

	if safety is not 'on':
		utils.initDB()
		for i in del_l:
			utils.dictcursor().execute(i)

		utils.replaceDB('schedconfig',up_l,key='nickname')
		utils.replaceDB('jdllist',jdl_l,key='name')
		utils.commit()
		
	newdb, sk = sqlDictUnpacker(loadSchedConfig())

	checkUp, checkDel = compareQueues(collapseDict(newdb), collapseDict(dbd))
	if genDebug: return dbd, configd, up_d, del_d, del_l, up_l, jdl_l, newjdl, newdb, checkUp, checkDel
	return 0
	

if __name__ == "__main__":
	keydict={}
	def testDiff(m,n):
		
		for i in up_d:
			try:
				for k in m[i].keys():
					if k not in ['jdladd','releases']:
						if m[i][k] != n[i][k]:
							print i, k, m[i][k], n[i][k], type(m[i][k]), type(n[i][k])
						
			except KeyError:
				pass

	# All of the passed dictionaries will be eliminated at the end of debugging. Necessary for now.
	dbd, configd, up_d, del_d, del_l, up_l, jdl_l, newjdl, newdb, checkUp, checkDel = loadConfigs()

	os.chdir(base_path)
