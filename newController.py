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
# fillDDMpaths?
# Add manual pickle restoration code
# Add subversion update, file add and checkin. Comment changes.
# Add change detection to avoid DB change collisions
# Add queue insertion scripts
# Add checking of queue "on" and "off"
# Make sure that the All source is subordinate to the BDII source

# This code has been organized for easy transition into a class structure.

import os, sys, commands

from controllerSettings import *
from miscUtils import *
from dbAccess import *
from dictHandling import *
from Integrators import *
from configFileHandling import *
from jdlController import *

if __name__ == "__main__":
	keydict={}
	def testDiff():
		
		for i in up_d:
			try:
				for k in m[i].keys():
					if k not in ['jdladd','releases']:
						if m[i][k] != n[i][k]:
							print i, k, m[i][k], n[i][k], type(m[i][k]), type(n[i][k])
						
			except KeyError:
				pass

	#cloudd = sqlDictUnpacker(unPickler('pickledSchedConfig.p'))
	# Load the present status of the DB, and describe a standard list of keys

	jdld = {}
	jdlListAdder(jdld)
	buildJdlFiles(jdld)
	newjdl=buildJdlDict()

	dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())
	# Load the present config files
	configd = buildDict()
	# Add the BDII information
	bdiiIntegrator(configd, dbd)
	# Now add ToA information to the whole shebang. No site-by-site as of yet.
	toaIntegrator(configd)

	# Compose the "All" queues for each site
	status = allMaker(configd)

	# Compare the DB to the present built configuration
	m = collapseDict(dbd)
	n = collapseDict(configd)
	up_d, del_d = compareQueues(collapseDict(dbd), collapseDict(configd))

	u,d=compareQueues(collapseDict(dbd),collapseDict(configd))
	a=buildDeleteList(d,'atlas_pandameta.schedconfig')
	b=buildUpdateList(u,'atlas_pandameta.schedconfig')

	os.chdir(base_path)
