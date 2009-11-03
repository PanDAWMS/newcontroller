#! /usr/bin/env python
#######################################################
# Handles storage and modification of queue data      #
# in the ATLAS PanDA schedconfig and jdllist tables   #
# for ATLAS production and analysis queues.           #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 23 Jun 09 #
#######################################################

# TODO:

# Make sure there's not a cloud-general All file
# Add a consistency checker
# Add pickling of db status for SVN
# Add manual pickle restoration code
# Add code for comparison between DB versions
# Add subversion update, file add and checkin. Comment changes.
# Add change detection to avoid DB change collisions
# Add logging output for changes and status
# Add queue insertion scripts
# Add empty site and cloud cleanup
# Add code for handling the jdllist (jdltext) table (field)
# Change to flexible mount point
# Make sure manual queues remain unmodified by BDII!
# Add checking of queue "on" and "off"
# BDII adding queues to clouds and sites -- note and copy parameters that apply to all. Then set offline and wait for mods.

# This code has been organized for easy transition into a class structure.

import pickle, os, sys, commands

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

try:
	import fillDDMpaths
except:
	print "Cannot import fillDDMpaths, will exit"
	sys.exit(-1)

safety = "on"
All = 'All'
ndef = 'Not_Defined'
param = 'Parameters'
over = 'Override'
source = 'Source'
base_path = os.getcwd()
backupPath = base_path + 'Backup'
backupName = 'schedConfigBackup.pickle'
configs = base_path + os.sep + 'Configs'
postfix = '.py'
dbkey, dsep, keysep, pairsep, spacing = 'nickname', ' : ', "'", ',', '    '  # Standard python spacing of 4
shared, unshared = 'shared','unshared'
excl = ['status','lastmod','dn','tspace']
standardkeys=[]

def loadSchedConfig():
	'''Returns the values in the schedconfig db as a dictionary'''
	utils.initDB()
	print "Init DB"
	query = "select * from schedconfig"
	nrows = utils.dictcursor().execute(query)
	if nrows > 0:
		rows = utils.dictcursor().fetchall()
	utils.endDB()
	d={}
	for i in rows:
		d[i[dbkey]]=i

	return d


# To be completed!! Needs to warn on lcgLoad missing
def loadBDII():
	'''Loads LCG site definitions from the BDII, and dumps them in a file called lcgQueueUpdate.py in the local directory.
	This file is executed (even if generating it failed this time) and populated a dictionary of queue definitions, which is
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
			lcgsites = osgsites
		else:
			loadlcg = 0
		return lcgsites
	  
# To be completed!!
def loadToA(queuedefs):
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

def protoDict(queue,d,sourcestr='DB',keys=[]):
	'''Create a dictionary with params, overrides and sources for either an existing definition from the DB, or to add the dictionaries
	for a new queue. Used in sqlDictUnpacker for extraction of DB values (default) and in bdiiIntegrator for new queue addition from the BDII.'''
	if not len(d):
		d={queue:dict([(key,'') for key in keys])}
	return {param:d[queue],over:{},source:dict([(i,sourcestr) for i in d[queue].keys() if key not in excl])}
	
def sqlDictUnpacker(d):
	'''Unpack the dictionary returned by Oracle or MySQL''' 
	# Organize by cloud, then site, then queue. Reading necessary data by key.
	# Remember that these vars are limited in scope.
	cloud = 'cloud'
	site = 'site'
	out_d={}
	# Run over the DB queues
	for queue in d:
		# If the present queue's cloud isn't in the out_d, create the cloud.
		if d[queue][cloud] not in out_d:
			out_d[d[queue][cloud]] = {}
		# If the present queue's site isn't in the out_d cloud, create the site in the cloud.
		if d[queue][site] not in  out_d[d[queue][cloud]]:
			out_d[d[queue][cloud]][d[queue][site]] = {}

		# Once sure that we have all the cloud and site dictionaries created, we populate them with a parameter dictionary
		# an empty (for now) override dictionary, and a source dict. The override dictionary will become useful when we are reading back from
		# the config files we are creating. Each key is associated with a source tag -- Config, DB, BDII, ToA, Override, Site, or Cloud
		# That list comprehension at the end of the previous line just creates an empty dictionary and fills it with the keys from the queue
		# definition. The values are set to DB, and will be changed if another source modifies the value.
		out_d[d[queue][cloud]][d[queue][site]][d[queue][dbkey]] = protoDict(queue,d)

	# Model keyset for creation of queues from scratch
	standardkeys=[key for key in d[queue].keys() if key not in excl]
	return out_d

def reducer(l):
	''' Reduce the entries in a list by removing dupes'''
	return dict([(i,1) for i in l]).keys()


def findQueue(q,d):
	'''Find a queue in the config dictionary and return the cloud and site. If not found, return empty values'''
	for cloud in d:
		for site in d[cloud]:
			# Not going to find sites in the All file
			if site = All:
				continue
			for queue in d[cloud][site]:
				if queue == q:
					return cloud, site
	return '',''

def bdiiIntegrator(d,confd):
	'''Adds BDII values to the configurations, overriding what was there. Must be run after downloading the DB
	and parsing the config files.'''
	out = {}
	# Load the queue names, status, gatekeeper, gstat, region, jobmanager, site, system, jdladd 
	bdict = loadBDII()
	# Load the site information directly from BDII and hold it. In the previous software, this was the osgsites dict.
	# This designation is obsolete -- this is strictly BDII information, and no separation is made.
	linfotool = lcgInfositeTool.lcgInfositeTool()
	for qn in bdict:
		# Create the nickname of the queue using the queue designation from the dict, plus the jobmanager.
		nickname = qn + '-' + bdict[qn]['jobmanager']
		# Try to find the queue in the DB dictionary
		c,s = findQueue(nickname,d)
		# If it's not there, try the dictionary from the previous config files
		if not c and not s:
			c,s = findQueue(nickname,confd)
			# If the queue is not in the DB, and is not inactive in the config files, then:
			if not c and not s:
			c,s = ndef,bdict[qn]['site']
			# If site doesn't yet exist, create it:
			if s not in d[c]:
				d[c][s] = {}
				# Create it in the main dictionary, using the standard keys from the DB
				d[c][s][nickname] = protoDict(nickname,{},sourcestr='BDII',keys=standardkeys)
			# Either way, we need to put the queue in without a cloud defined. 
		# For all the simple translations, copy them in directly.
		for key in ['localqueue','system','status','gatekeeper','jobmanager','jdladd','site','region','gstat']:
			d[c][s][nickname][param][key] = bdict[qn][key]
			# Complete the sourcing info
			d[c][s][nickname][source][key] = 'BDII'
		# For the more complicated BDII derivatives, do some more complex work
		d[c][s][nickname][param]['queue'] = bdict[qn][key] + '/jobmanager-' + bdict[qn]['jobmanager']
		d[c][s][nickname][param]['jdl'] = bdict[qn][key] + '/jobmanager-' + bdict[qn]['jobmanager']
		d[c][s][nickname][param]['nickname'] = nickname
		# Fill in sourcing here as well for the last few fields
		for key in ['queue','jdl','nickname']:
			d[c][s][nickname][source][key] = 'BDII'
	# All changes to the dictionary happened live -- no need to return it.
	return 0

def allMaker(d):
	'''Extracts commonalities from sites and clouds for the All files.
	Returns its own dictionary for constructing these files. Updates the
	provenance info in the input dictionary. '''
	
	out = {}
	# This is where we'll put all verified keys that are common across sites/clouds
	for cloud in [i for i in d.keys() if (i is not All and i is not ndef)]:
		# Create a dictionary for each cloud (and its sites), and a dict for values common to sites.
		out[cloud]={All:{}}
		# This buffers the key comparison lists for all queues in a cloud, not just per site
		ccomp = {}
		# For all regular sites:
		for site in [i for i in d[cloud].keys() if (i is not All and i is not ndef)]:
			# Set up a site output dictionary
			out[cloud][site]={}
			# Recreate the site comparison queue
			comp = {}
			# Loop over all the queues in the site, where the queue is not empty or "All"
			for queue in [i for i in d[cloud][site].keys() if (i is not All and i is not ndef)]:
				# Create the key in the comparison dictionary for each parameter, if necessary, and assign a list that will hold the values from each queue 
				if not len(comp): comp = dict([(i,[d[cloud][site][queue][param][i]]) for i in d[cloud][site][queue][param].keys()])
				if not len(ccomp): ccomp = dict([(i,[d[cloud][site][queue][param][i]]) for i in d[cloud][site][queue][param].keys()])
				else: 
					# Fill the lists with the values for the keys from this queue
					for key in d[cloud][site][queue][param]:
						if key not in excl:
							comp[key].append(d[cloud][site][queue][param][key])
							ccomp[key].append(d[cloud][site][queue][param][key])
			# Now, for the site, remove all duplicates in the lists. 
			for key in comp:
				# If only one value is left, it is common to all queues in the site
				if len(reducer(comp[key])) == 1:
					# So write it to the output for this cloud and site.
					out[cloud][site][key] = reducer(comp[key])[0]
		# Doing the same as above per cloud:   
		for key in ccomp:
			# If only one value is left, it is common to all queues in the cloud
			if len(reducer(ccomp[key])) == 1:
				# So write it to the output for this cloud.
				out[cloud][All][key] = reducer(ccomp[key])[0]

	# Running across clouds and sites to update source information in the main dictionary
	for cloud in [i for i in d.keys() if (i is not All and i is not ndef)]:
		# Extract all affected keys for the cloud
		ckeys = out[cloud][All].keys()
		for site in [i for i in d[cloud].keys() if (i is not All and i is not ndef)]:
			# Extract all affected keys for the site
			skeys = out[cloud][site].keys()
			# Going queue by queue, update the provenance for both cloud and site general parameters.
			for queue in [i for i in d[cloud][site].keys() if (i is not All and i is not ndef)]:
				for key in ckeys:
					d[cloud][site][queue][source][key] = 'the cloud All.py file for the %s cloud' % cloud
				for key in skeys:
					d[cloud][site][queue][source][key] = 'the site All.py file for the %s site' % site
		
	return out

def composeFile(d,s,dname):
	''' Populate a list for file writing that prints parameter dictionaries cleanly,
	allowing them to be written to human-modifiable config files for queues and sites.'''

	# "dname" is one of two things -- "Parameters" or "Override", depending on what part of the  
	# file we're writing. They're defined generally as param and over 
	keylist = d[dname].keys()
	try:
		# Remove the DB key and put in as the first parameter -- this will be "nickname", usually.
		keylist.remove(dbkey)
		keylist.sort()
		if dname == param: keylist.insert(0,dbkey)
		
	# Unless it's not present -- then we'll just throw a warning.	 
	except ValueError:
		if dname == param: print 'DB key %s not present in this dictionary. Going to be hard to insert. %s' % (dbkey, str(d))
		keylist.sort()

	# So we're writing a  "Parameters" or "Override" dictionary (dname)...
	s.append('%s = {' % dname + os.linesep )
	for key in keylist:
		comment = ['','']
		value = str(d[dname][key])
		# For each key and value, check for multiline values, and add triple quotes when necessary 
		if value.count(os.linesep): valsep = "'''"
		else: valsep = keysep
		# Add a comment hash to the line, and add the provenance info 
		if dname == param and d.has_key(source) and d[source][key] is not 'DB':
			comment = ['#','# Defined in %s' % d[source][key]]
		# Build the text, with linefeeds, and add it to the out string.
		s.append(spacing + comment[0] + keysep + key + keysep + dsep + valsep + value + valsep + pairsep + comment[1] + os.linesep)
	# Complete the dictionary
	s.append(spacing + '}' + os.linesep)
	s.append(os.linesep)
	return s

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

	# Load up the file intro
	s=[startstr]
	# Put the queue on/off switch in place if not an All file
	if name is not All: s.append(switchstr)
	# I'm taking advantage of the universality of lists.
	# composeFile is modifying the list itself rather than a copy.
	composeFile(d, s, param)
	s.append(overridestr)
	composeFile(d, s, over)
	
	f=file(name + postfix,'w')
	f.writelines(s)
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
		#merge = "MERGE INTO atlas_pandameta.%s USING DUAL ON ( atlas_pandameta.%s.nickname='%s' ) " % (tableName, tableName, i)
		merge = "MERGE INTO %s USING DUAL ON ( %s.nickname='%s' ) " % (tableName, tableName, i)
		mergetxt1 = ' (%s) ' % ','.join(['%s=:%s' % (i,i) for i in sorted(updDict[i].keys())])
		mergetxt2 = ' (%s) ' % ',:'.join(sorted(updDict[i].keys()))
		valuestxt = '{%s} ' % ', '.join(["'%s': '%s'" % (i,updDict[i]) for i in sorted(updDict[i].keys())])
		sql.append(merge+matched+insert+mergetxt1+values+mergetxt2+values2+valuestxt+';')
		
	return '/n'.join(sql)

def buildDeleteList(delDict, tableName):
	'''Build a list of SQL commands that deletes queues no longer in the definition files'''
	#delstr='DELETE FROM atlas_pandameta.%s WHERE NICKNAME = '
	delstr='DELETE FROM %s WHERE NICKNAME = '
	sql=[]
	for i in delDict:
		sql.append(delstr+i['nickname']+';')
	return '\n'.join(sql)

# To be completed!!
def buildDict():
	'''Build a copy of the queue dictionary from the configuration files '''
	confd={}
	# Loop throught the clouds in the base folder
	clouds = os.listdir(configs):
	for cloud in clouds
		# Add each cloud to the dictionary
		confd[cloud] = {}
		# Loop throught the sites in the present cloud folder
		sites = os.listdir(configs + os.sep + cloud)
		for site in clouds:
			# If this is the All file, create another entry.
			if site.endswith(postfix):
				# Get rid of the .py
				s=site.rstrip(postfix)
				# Run the file for the dictionaries
				execfile(configs + os.sep + cloud + os.sep + site)
				confd[cloud][s][param] = Parameters
				confd[cloud][s][override] = Override 
				# Delete the dictionaries for safety
				del(Parameters)
				del(Override)
			# Add each site to the cloud
			confd[cloud][site] = {}
			# Loop throught the queues in the present site folders
			queues = [i for i in os.listdir(configs + os.sep + cloud + os.sep + site) if i.endswith(postfix)]
			for q in queues:
				# Remove the '.py' 
				queue=q.rstrip(postfix)
				# Add each queue to the site
				confd[cloud][site][queue] = {}
				# Run the file to extract the appropriate dictionaries
				execfile(configs + os.sep + cloud + os.sep + site + os.sep + q)
				# Feed in the configuration
				confd[cloud][site][queue][param] = Parameters
				confd[cloud][site][queue][override] = Override 
				confd[cloud][site][queue]['Enabled'] = Enabled 				
				confd[cloud][site][queue][source] = dict([(key,'Config') for key in Parameters if key not in excl]) 				
				# Clear the values, for safety
				del(Parameters)
				del(Override)
				del(Enabled)
	# Leaving the All parameters unincorporated until the end.
	return confd

# To be completed!!
def execUpdate(updateList):
	''' Run the updates into the schedconfig database '''
	if safety is "on":
		print "Not touching the database! The safety's on ol' Bessie."
		return 1
	utils.initDB()
	for query in updateList:
		utils.dictcursor().execute(query)
	utils.commit()
	utils.closeDB()
	return loadSchedConfig()

# To be completed!!
def jdlListAdder(d):
	'''Merge the contents of the jdllist table (name, host, system, jdltext) into the schedconfig dictionary'''
	pass


if __name__ == "__main__":
	#cloudd = sqlDictUnpacker(unPickler('pickledSchedConfig.p'))
	cloudd = sqlDictUnpacker(loadSchedConfig())
## 	for cloud in cloudd:
## 		for site in cloudd[cloud]:
## 			for queue in cloudd[cloud][site]:
## 				print cloud, site, queue, cloudd[cloud][site][queue].keys()

	all_d = allMaker(cloudd)
	# Since many fields for both clouds and queues are lacking designations, allow them to be removed
	if cloudd.has_key(''):
		cloudd[ndef]=cloudd.pop('')
	if cloudd.has_key(None):
		cloudd[ndef]=cloudd.pop(None)
	for cloud in cloudd:
		try:
			cloudd[cloud][All] = {param:all_d[cloud][All], over:{}}
			for site in cloudd[cloud]:
				cloudd[cloud][site][All] = {param:all_d[cloud][site][All], over:{}}
		except KeyError:
			pass

	for cloud in cloudd:
		cpath = configs + os.sep + cloud
		try:
		  os.makedirs(cpath)
		except OSError:
			pass
		for site in cloudd[cloud]:
			if site is All:
				path = cpath
			else:
				path = cpath + os.sep + site
				try:
					os.makedirs(path)
				except OSError:
					pass
			os.chdir(path)

			if site is All:
				buildFile(All,cloudd[cloud][All])

			else:
				for queue in cloudd[cloud][site]:
					if queue is All: buildFile(queue, cloudd[cloud][site][queue])
					else: buildFile(queue, cloudd[cloud][site][queue])


	os.chdir(base_path)

