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

import pickle, os, sys, commands, re

from jdlController import *
from dictHandling import *
from configFileHandling import *
from dbAccess import *
from Integrators import *

from SchedulerUtils import utils

try:
	import dq2.info.TiersOfATLAS as ToA
except:
	print "Cannot import dq2.info.TiersOfATLAS, will exit"
	sys.exit(-1)

try:
	import lcgInfositeTool
except:
	print "Cannot import lcgInfositeTool, will exit"
	sys.exit(-1)

# Not used yet -- evaluate!
try:
	import fillDDMpaths
except:
	print "Cannot import fillDDMpaths, will exit"
	sys.exit(-1)


toaDebug = False
jdlDebug = False
bdiiDebug = False
dbReadDebug = False
dbWriteDebug = False
configReadDebug = False
configWriteDebug = False

safety = 'on'
All = 'All'
ndef = 'Deactivated'
param = 'Parameters'
over = 'Override'
jdl = 'JDL'
source = 'Source'
enab = 'Enabled'
base_path = os.getcwd()
# Step back a layer in the path for the configs, and put them in the config SVN directory
cfg_path = base_path[:base_path.rfind(os.sep)] + os.sep + 'pandaconf' + os.sep
backupPath = cfg_path + 'Backup'
backupName = 'schedConfigBackup.pickle'
configs = cfg_path + os.sep + 'SchedConfigs'
jdlconfigs = cfg_path + os.sep + 'JDLConfigs'
postfix = '.py'
dbkey, dsep, keysep, pairsep, spacing = 'nickname', ' : ', "'", ',', '    '  # Standard python spacing of 4
shared, unshared = 'shared','unshared'
excl = ['status','lastmod','dn','tspace','_comment']
standardkeys = []

#----------------------------------------------------------------------#
# Utilities
#----------------------------------------------------------------------#

def unPickler(fname):
	os.chdir(base_path)
	f=file(fname)
	t=pickle.load(f)
	f.close()
	d={}
	for i in t:
		d[i[dbkey]]=i
	return d
	
def pickleBackup(d):
	'''Pickle the schedconfigdb as a backup'''
	try:
		os.makedirs(backupPath)
	except OSError:
		pass
	os.chdir(backupPath)
	f=file(backupName, 'w')
	pickle.dump(d,f)
	f.close()

def reducer(l):
	''' Reduce the entries in a list by removing dupes'''
	return dict([(i,1) for i in l]).keys()



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
				print '\n\n********************** %s was not found in the db\n\n' % i

	#cloudd = sqlDictUnpacker(unPickler('pickledSchedConfig.p'))
	# Load the present status of the DB, and describe a standard list of keys

	jdld = {}
	jdlListAdder(jdld)
	buildJdlFiles(jdld)
	newjdl=buildJdlDict()

	dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())
	# Load the present config files
	configd = buildDict()
	
	# Compose the "All" queues for each site
	status = allMaker(configd)

	# Add the BDII information
	bdiiIntegrator(configd, dbd)
	# Now add ToA information to the whole shebang. No site-by-site as of yet.
	toaIntegrator(configd)

	# Compare the DB to the present built configuration
	up_d, del_d = compareQueues(collapseDict(dbd), collapseDict(configd))

	m,n=collapseDict(dbd),collapseDict(configd)
	u,d=compareQueues(collapseDict(dbd),collapseDict(configd))
	a=buildDeleteList(d,'atlas_pandameta.schedconfig')
	b=buildUpdateList(u,'atlas_pandameta.schedconfig')

	os.chdir(base_path)
		
		
