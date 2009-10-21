#! /usr/bin/env python
#######################################################
# Handles storage and modification of queue data      #
# in the ATLAS PanDA schedconfig and jdllist tables   #
# for ATLAS production and analysis queues.           #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 23 Jun 09 #
#######################################################

# TODO:

# Add a consistency checker
# Set fields not to touch: DATE AND TIME STAMPS, modification user, status
# Add DB acquisition code
# Add pickling of db status for SVN
# Add manual pickle restoration code
# Add code for comparison between DB versions
# Add subversion update, file add and checkin. Comment changes.
# Add change detection to avoid DB change collisions
# --- Priorities: Manual changes in SVN. DB backwash provosions can be added later
# Add row-specific updates via change detection between old and new versions
# Add logging output for changes and status
# Add queue insertion scripts
# Add code for manual cleanup of sites without cloud IDs
# Add code for manual cleanup of queues with siteid "?" or ""
# Add code for merging sites by hand
# Add deletion of nonexistent queues
# Add BDII override. 
# Add function for parsing and uploading the dictionary
# Add code for handling the jdllist (jdltext) table (field)
# Change to flexible mount point

# This code has been organized for easy transition into a class structure.

import pickle, os, sys

#from SchedulerUtilsimport utils

## try:
## 	import dq2.info.TiersOfATLAS as ToA
## except:
## 	print "Cannot import dq2.info.TiersOfATLAS, will exit"
## 	sys.exit(-1)

## try:
## 	import lcgInfositeTool
## except:
## 	print "Cannot import lcgInfositeTool, will exit"
## 	sys.exit(-1)

## try:
## 	import fillDDMpaths
## except:
## 	print "Cannot import fillDDMpaths, will exit"
## 	sys.exit(-1)



all = 'All'
none = 'None'
over = 'Override'
param = 'Parameters'
base_path = os.getcwd()
backupPath = base_path + 'Backup'
backupName = 'schedConfigBackup.pickle'
configs = base_path + 'Configs'
postfix = '.py'
dbkey, dsep, keysep, pairsep, spacing = 'nickname', ' : ', "'", ',', '    '  # Standard python spacing of 4
shared, unshared = 'shared','unshared'

def loadBDII():
	'''Loads LCG site definitions from the BDII, and dumps them in a file called lcgQueueUpdate.py in the local directory.
	This file is executed (even in generating it failed this time) and populated a dictionary of queue definitions, which is
	returned.'''
	osgsites={}
	if os.path.exists('lcgLoad.py'):
		print "Updating LCG sites from BDII"
		try:
			print commands.getoutput('./lcgLoad.py')
		except Exception, e:
			print "Running lcgLoad.py failed:", e
			print "Reusing existing lcgQueueUpdate.py"
			execfile('lcgQueueUpdate.py')
	else:
		loadlcg = 0
		
	return osgsites

# To be completed!!
def loadTOA(queuedefs):
	'''Acquires queue config information from ToA and updates the values we have. Should be run last. Overrides EVERYTHING else.'''
	fillDDMpaths.fillDDMpaths(queuedefs)
	return 0

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
	return
    
# Organize by cloud, then site, then queue. Reading necessary data by key.
# Remember that these vars are limited in scope.
def sqlDictUnpacker(d):
	'''Unpack the queue listing dictionary returned by Oracle or MySQL from the schedconfig table''' 
	cloud = 'cloud'
	site = 'site'
	out_d = {}
	for queue in d:
		if d[queue][cloud] not in out_d:
			out_d[d[queue][cloud]] = {}
		if d[queue][site] not in  out_d[d[queue][cloud]]:
			out_d[d[queue][cloud]][d[queue][site]] = {}
        # Once sure that we have all the cloud and site dictionaries created, we popupate them with a parameter dictionary
        # and an empty (for now) override dictionary. The override dictionary will become useful when we are reading back from
        # the config files we are creating.
		out_d[d[queue][cloud]][d[queue][site]][d[queue][dbkey]] = {param:d[queue],over:{}}
	return out_d

def reducer(l):
	''' Reduce the entries in a list by removing dupes'''
	return dict([(i,1) for i in l]).keys()

def allMaker(d):
	''' Extracts commonalities from sites and clouds for the All files.'''
	out = {}
	# This is where we'll put all verified keys that are common across sites
	for cloud in [i for i in d.keys() if (i is not all and i is not none)]:
		# Create a dictionary for each cloud (and its sites), and a dict for values common to sites.
		out[cloud]={all:{}}
		# This buffers the key comparison lists for all queues in a cloud, not just per site
		cloudcomp = {}
		# For all regular sites:
		for site in [i for i in d[cloud].keys() if (i is not all and i is not none)]:
			# Set up a site output dictionary
			out[cloud][site]={}
			# Recreate the site comparison queue
			comp = {}
			# Loop over all the queues in the site, where the queue is not empty or "All"
			for queue in [i for i in d[cloud][site].keys() if (i is not all and i is not none)]:
				# Create the key in the comparison dictionary for each parameter, if necessary, and assign a list that will hold the values from each queue 
				if not len(comp): comp = dict([(i,[d[cloud][site][queue][param][i]]) for i in d[cloud][site][queue][param].keys()])
				if not len(cloudcomp): cloudcomp = dict([(i,[d[cloud][site][queue][param][i]]) for i in d[cloud][site][queue][param].keys()])
				else: 
					# Fill the lists with the values for the keys from this queue
					for key in d[cloud][site][queue][param]:
						comp[key].append(d[cloud][site][queue][param][key])
						cloudcomp[key].append(d[cloud][site][queue][param][key])
			# Now, for the site, remove all duplicates in the lists. 
			for key in comp:
				# If only one value is left, it is common to all queues in the site
				if len(reducer(comp[key])) == 1:
					# So write it to the output for this cloud and site.
					out[cloud][site][key] = reducer(comp[key])[0]
		# Doing the same as above per cloud:   
		for key in cloudcomp:
			# If only one value is left, it is common to all queues in the cloud
			if len(reducer(cloudcomp[key])) == 1:
				# So write it to the output for this cloud.
				out[cloud][all][key] = reducer(cloudcomp[key])[0]

	return out

def composeFile(d,l,dname):
	# Be sure to add commenting of BDII, ToA and removal of other fields
	''' Populate a list for file writing that prints parameter dictionaries cleanly,
	allowing them to be written to human-modifiable config files for queues and sites.'''
	keylist = d[dname].keys()
	try:
		keylist.remove(dbkey)
		keylist.sort()
		if dname == param: keylist.insert(0,dbkey)
	except ValueError:
		keylist.sort()
		
	l.append('%s = {' % dname + os.linesep )
	for key in keylist:
		value = str(d[dname][key])
		if value.count(os.linesep): valsep = "'''"
		else: valsep = keysep
		l.append(spacing + keysep + key + keysep + dsep + valsep + value + valsep + pairsep + os.linesep)
	l.append(spacing + '}' + os.linesep)
	l.append(os.linesep)
	return l

def buildFile(name, d):
	'''Consolidate the composition and writing of the files'''
	startstr = '''
# This dictionary contains the parameters for one queue.
# Changing any parameter will update it in the schedconfig database.
# If you want to change a value temporarily, preserving the previous
# value, please put that parameter (key and new value) in the Override
# dictionary. Any value in Override will supersede any value in the
# Parameters dictionary.

# Global values for entire sites and clouds can also be set
# (and overriden) in the "All" files within these directories.

'''
	overridestr = '''
# PLEASE USE FOR TEMPORARY CHANGES TO A QUEUE OR SITE
# This dictionary will override any queue value within its scope.
# If this override dictionary is part of an "All" file,
# key:value pairs included in it will override all other
# "Parameter" values. "Override" values in individual queues
# will supersede values specified here.

'''

	switchstr = 'Enabled = True\n\n'
	
	l=[startstr]
	if name is not all: l.append(switchstr)
	# I'm taking advantage of the universality of lists.
	# composeFile is modifying the list itself rather than a copy.
	composeFile(d, l, param)
	l.append(overridestr)
	composeFile(d, l, over)
	
	f=file(name + postfix,'w')
	f.writelines(l)
	f.close()

def compareQueues(dbDict,cfgDict,dbOverride=False):
	'''Compares the queue dictionary that we got from the DB to the one in the config files. Any changed/deleted
	queues are passed back. If dbOverride is set true, the DB is used to modify the config files rather than
	the default. Queues deleted in the DB will not be deleted in the configs.'''
	updDict = {}
	delDict = {}
	# Allow the reversal of master and subordinate dictionaries
	if dbOverride: dbDict, cfgDict = cfgDict, dbDict
	if dbDict == cfgDict: return updDict, delDict
	for i in dbDict:
		if not cfgDict.has_key(i):
			# Queues must be deleted in the configs.
			if not dbOverride:
				# If the queue is not in the configs, delete it.
				delDict[i]=dbDict[i]
				continue
		if dbDict[i]!=cfgDict[i]:
			# If the queue was changed in the configs, tag it for update.
			updDict[i]=cfgDict[i]

	# Return the appro
	return updDict, cfgDict
  
def buildUpdateList(updDict, tableName):
	'''Build a list of SQL commands to add or supersede queue definitions''' 

	matched = ' WHEN MATCHED THEN UPDATE SET '
	insert = ' WHEN NOT MATCHED THEN INSERT '
	values1 = ' VALUES '
	values2 = ' WITH VALUES '
	sql = []
	for i in updDict:
		merge = "MERGE INTO ATLAS_PANDAMETA.%s USING DUAL ON ( ATLAS_PANDAMETA.%s.nickname='%s' ) " % (tableName, tableName, i)
		mergetxt1 = ' (%s) ' % ','.join(['%s=:%s' % (i,i) for i in sorted(updDict[i].keys())])
		mergetxt2 = ' (%s) ' % ',:'.join(sorted(updDict[i].keys()))
		valuestxt = '{%s} ' % ', '.join(["'%s': '%s'" % (i,updDict[i]) for i in sorted(updDict[i].keys())])
		sql.append(merge+matched+insert+mergetxt1+values+mergetxt2+values2+valuestxt+';')

	return '/n'.join(sql)

def buildDeleteList(delDict, tableName):
	'''Build a list of SQL commands that deletes queues no longer in the definition files'''
	delstr='DELETE FROM ATLAS_PANDAMETA.%s WHERE NICKNAME = '
	sql=[]
	for i in delDict:
	  sql.append(delstr+i['nickname']+';')
	return '\n'.join(sql)

# To be completed!!
def jdlListAdder(d):
	'''Merge the contents of the jdllist table (name, host, system, jdltext) into the schedconfig dictionary'''
	pass

os.chdir(base_path)

cloudd = sqlDictUnpacker(unPickler(base_path+os.sep+'pickledSchedConfig.p'))

all_d = allMaker(cloudd)

if cloudd.has_key(''):
	cloudd[none]=cloudd.pop('')
for cloud in cloudd:
	try:
		cloudd[cloud][all] = {param:all_d[cloud][all], over:{}}
		for site in cloudd[cloud]:
			cloudd[cloud][site][all] = {param:all_d[cloud][site][all], over:{}}
	except KeyError:
		pass

for cloud in cloudd:
	cpath = configs + os.sep + cloud
	try:
		os.makedirs(cpath)
	except OSError:
		pass
	for site in cloudd[cloud]:
		if site is all:
			path = cpath
		else:
			path = cpath + os.sep + site
			try:
				os.makedirs(path)
			except OSError:
				pass

		os.chdir(path)

		if site is all:
			buildFile(all,cloudd[cloud][all])

		else:
			for queue in cloudd[cloud][site]:
				if queue is all: buildFile(queue, cloudd[cloud][site][queue])
				else: buildFile(queue, cloudd[cloud][site][queue])

os.chdir(base_path)
