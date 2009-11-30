#! /usr/bin/env python
#######################################################
# Handles storage and modification of queue data      #
# in the ATLAS PanDA schedconfig and jdllist tables   #
# for ATLAS production and analysis queues.           #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 23 Jun 09 #
#######################################################

# TODO:

# Add ToA handling
# Add installedSW table support
# Add a consistency checker
# Add manual pickle restoration code
# Add code for comparison between DB versions
# Add subversion update, file add and checkin. Comment changes.
# Add change detection to avoid DB change collisions
# Add logging output for changes and status
# Add queue insertion scripts
# Add code for handling the jdllist (jdltext) table (field)
# Change to flexible mount point
# Make sure manual queues remain unmodified by BDII!
# Add checking of queue "on" and "off"
# BDII adding queues to clouds and sites -- note and copy parameters that apply to all. Then set offline and wait for mods.
# Make sure that the All source is subordinate to the BDII source
# Make sure that the jdladd field is fully commented when it's being removed from the source

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
ndef = 'Deactivated'
param = 'Parameters'
over = 'Override'
source = 'Source'
enab = 'Enabled'
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
		print 'Updating LCG sites from BDII'
		try:
			commands.getoutput('./lcgLoad.py > lcgload.log')
		except Exception, e:
			print 'Running lcgLoad.py failed:', e
			print 'Reusing existing lcgQueueUpdate.py'
		execfile('lcgQueueUpdate.py')
		print 'LCG Initial Load Complete'
	else:
		loadlcg = 0
	return osgsites
	  
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
	e = 'True'
	if not len(d):
		e = 'False'
		d = {queue:dict([(key,'') for key in keys])}
	return {param:d[queue],over:{},source:dict([(key,sourcestr) for key in d[queue].keys() if key not in excl]),enab:e}
	
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
	# Parse the dictionary to create an All queue for each site
	status = allMaker(out_d)
	# Take care of empty clouds (which are used to disable queues in schedconfig, for now) 
	# allMaker has to run before this to avoid causing KeyErrors with the new "empty cloud" values 
	if out_d.has_key(''):
		out_d[ndef]=out_d.pop('')
	if out_d.has_key(None):
		out_d[ndef]=out_d.pop(None)
		
	return out_d

def reducer(l):
	''' Reduce the entries in a list by removing dupes'''
	return dict([(i,1) for i in l]).keys()


def findQueue(q,d):
	'''Find a queue in the config dictionary and return the cloud and site. If not found, return empty values'''
	for cloud in d:
		for site in d[cloud]:
			# Not going to find sites in the All file
			if site == All:
				continue
			for queue in d[cloud][site]:
				if queue == q:
					return cloud, site
	return '',''

def bdiiIntegrator(confd,d):
	'''Adds BDII values to the configurations, overriding what was there. Must be run after downloading the DB
	and parsing the config files.'''
	out = {}
	# Load the queue names, status, gatekeeper, gstat, region, jobmanager, site, system, jdladd 
	bdict = loadBDII()
	# Moving on from the lcgLoad sourcing, we extract the RAM, nodes and releases available on the sites 
	print 'Running the LGC SiteInfo tool'
	linfotool = lcgInfositeTool.lcgInfositeTool()
	print 'Completed the LGC SiteInfo tool run'

	# Defining a release dictionary
	rellist={}
	
	# Load the site information directly from BDII and hold it. In the previous software, this was the osgsites dict.
	# This designation is obsolete -- this is strictly BDII information, and no separation is made.
	for qn in bdict:
		# Create the nickname of the queue using the queue designation from the dict, plus the jobmanager.
		nickname = '-'.join([qn,bdict[qn]['jobmanager']]).rstrip('-')
		# Try to find the queue in the configs dictionary
		c,s = findQueue(nickname,confd)
		if not c and not s:
			# If it's not there, try the dictionary from the DB dictionary
			c,s = findQueue(nickname,d)
			# If the queue is not in the DB, and is not inactive in the config files, then:
			if not c and not s:
				# If a site is specified, go ahead
				if bdict[qn]['site']: c,s = ndef,bdict[qn]['site']
				# If not, time to give up. BDII is hopelessly misconfigured -- best not to contaminate
				else: continue
				# If site doesn't yet exist, create it:
				if s not in confd[c]:
					confd[c][s] = {}
					# Create it in the main config dictionary, using the standard keys from the DB (set in the initial import)
					confd[c][s][nickname] = protoDict(nickname,{},sourcestr='BDII',keys=standardkeys)
					confd[c][s][nickname][enab] = 'False'
			# Either way, we need to put the queue in without a cloud defined. 
		# Check for manual setting. If it's manual, DON'T TOUCH
## 		if confd[c][s][nickname][param]['sysconfig'].lower() == 'manual':
## 			continue

		# Make sure the cloud and site are even defined.
		if c not in confd:
			confd[c] = {}
		if s not in confd[c]:
			confd[c][s] = {}
		# If the queue is not present even after all that, add it. 
		if dbkey not in confd[c][s]:
			confd[c][s][nickname] = protoDict(nickname,{},sourcestr='BDII',keys=standardkeys)
			confd[c][s][nickname][enab] = 'False'
		# For all the simple translations, copy them in directly.
		for key in ['localqueue','system','status','gatekeeper','jobmanager','jdladd','site','region','gstat']:
			confd[c][s][nickname][param][key] = bdict[qn][key]
			# Complete the sourcing info
			confd[c][s][nickname][source][key] = 'BDII'
		# For the more complicated BDII derivatives, do some more complex work
		confd[c][s][nickname][param]['queue'] = bdict[qn][key] + '/jobmanager' + bdict[qn]['jobmanager']
		confd[c][s][nickname][param]['jdl'] = bdict[qn][key] + '/jobmanager-' + bdict[qn]['jobmanager']
		confd[c][s][nickname][param]['nickname'] = nickname
		# Fill in sourcing here as well for the last few fields
		for key in ['queue','jdl','nickname']:
			confd[c][s][nickname][source][key] = 'BDII'
		
		tags=linfotool.getSWtags(confd[c][s][nickname][param]['gatekeeper'])
		etags=linfotool.getSWctags(confd[c][s][nickname][param]['gatekeeper'])
## 		if len(etags) > 0:
## 			for tag in etags:
## 				try:
## 					cache=tag.replace('production','AtlasProduction').replace('tier0','AtlasTier0') 
## 					release='.'.join(tag.split('-')[1].split('.')[:-1])
## 					idx = '%s_%s' % (confd[c][s][nickname][param]['site'],cache)
## 					rellist[idx]=dict([('site',confd[c][s][nickname][param]['site']),('cloud',confd[c][s][nickname][param]['cloud'])])
## 					rellist[idx]['release'] = release
## 					rellist[idx]['cache'] = cache
## 					rellist[idx]['siteid'] = '' # to fill later, when this is available
## 					rellist[idx]['nickname'] = confd[c][s][nickname][param]['nickname'] # To reference later, when we need siteid
## 					rellist[idx]['gatekeeper'] = gk
## 				except KeyError:
## 					print confd[c][s][nickname][param]
## 		else:
## 			print("No eTags!")

		# Needs work!!!
		if len(tags) > 0:
			for release in tags:
				try:
					idx = '%s_%s' % (confd[c][s][nickname][param]['site'],release)
					rellist[idx]={'site':confd[c][s][nickname][param]['site']}
					rellist[idx]['release'] = release
					rellist[idx]['cache'] = ''
					rellist[idx]['siteid'] = '' # to fill later, when this is available
					rellist[idx]['nickname'] = confd[c][s][nickname][param]['nickname'] # To reference later, when we need siteid
					rellist[idx]['gatekeeper'] = confd[c][s][nickname][param]['gatekeeper']
				except KeyError:
					pass
					#print release, idx, confd[c][s][nickname][param]
		else:
			#print("No Tags for %s!" % nickname)
			pass
			
		if len(tags) > 0:
			releases = '|'.join(tags)
			confd[c][s][nickname][param]['releases']=releases
			confd[c][s][nickname][source]['releases'] = 'BDII'
		else:
			#print "No releases found for %s"% confd[c][s][nickname][param]['gatekeeper']
			pass


## 		# validatedreleaeses for reprocessing
## 		# not here but set manually in sqlplus
## 		# Fill the RAM
## 		confd[c][s][nickname][param]['memory'] = linfotool.getRAM(confd[c][s][nickname][param]['gatekeeper'],confd[c][s][nickname][param]['localqueue'])
## 		confd[c][s][nickname][param]['maxcpu'] = linfotool.getMaxcpu(confd[c][s][nickname][param]['gatekeeper'],confd[c][s][nickname][param]['localqueue'])
## 		if confd[c][s][nickname][param]['site'] in ['IN2P3-LAPP']:
## 			confd[c][s][nickname][param]['maxtime'] = maxcpu  

	
	# All changes to the dictionary happened live -- no need to return it.
	return 0

def allMaker(d):
	'''Extracts commonalities from sites for the All files.
	Returns 0 for success. Adds "All" queues to sites. Updates the
	provenance info in the input dictionary. '''
	
	all_d = {}
	# This is where we'll put all verified keys that are common across sites/clouds
	for cloud in [i for i in d.keys() if (i is not All and i is not ndef)]:
		# Create a dictionary for each cloud 
		all_d[cloud]={}
		# For all regular sites:
		for site in [i for i in d[cloud].keys() if (i is not All and i is not ndef)]:
			# Set up a site output dictionary
			all_d[cloud][site]={}
			# Recreate the site comparison queue
			comp = {}
			# Loop over all the queues in the site, where the queue is not empty or "All"
			for queue in [i for i in d[cloud][site].keys() if (i is not All and i is not ndef)]:
				# Create the key in the comparison dictionary for each parameter, if necessary, and assign a list that will hold the values from each queue 
				if not len(comp): comp = dict([(i,[d[cloud][site][queue][param][i]]) for i in d[cloud][site][queue][param].keys() if i not in excl])
				else: 
					# Fill the lists with the values for the keys from this queue
					for key in d[cloud][site][queue][param]:
						if key not in excl:
							try:
								comp[key].append(d[cloud][site][queue][param][key])
							except KeyError:
								comp[key] = [d[cloud][site][queue][param][key]]

			# Now, for the site, remove all duplicates in the lists. 
			for key in comp:
				# If only one value is left, it is common to all queues in the site
				if len(reducer(comp[key])) == 1:
					# So write it to the output for this cloud and site.
					all_d[cloud][site][key] = reducer(comp[key])[0]

	# Running across sites to update source information in the main dictionary
	for cloud in d.keys():
		for site in [i for i in d[cloud].keys() if (i is not All and i is not ndef)]:
			# No point in making an All file for one queue definition:
			if len(d[cloud][site]) > 1:
				# Extract all affected keys for the site
				skeys = all_d[cloud][site].keys()
				# Going queue by queue, update the provenance for both cloud and site general parameters.
				for queue in [i for i in d[cloud][site].keys() if (i is not All and i is not ndef)]:
					for key in skeys:
						d[cloud][site][queue][source][key] = 'the site All.py file for the %s site' % site
				# Adding the "All" queue to the site
				d[cloud][site][All] = {param:all_d[cloud][site]}
				d[cloud][site][All].update({over:{}})

	return 0

def composeFields(d,s,dname,allFlag=0):
	''' Populate a list for file writing that prints parameter dictionaries cleanly,
	allowing them to be written to human-modifiable config files for queues and sites.'''

	# "dname" is one of two things -- "Parameters" or "Override", depending on what part of the  
	# file we're writing. They're defined generally as param and over 
	keylist = d[dname].keys()
	try:
		# Remove the DB key and put in as the first parameter -- this will be "nickname", usually.
		keylist.remove(dbkey)
		keylist.sort()
		keylist.insert(0,dbkey)

	# Unless it's not present -- then we'll just throw a warning.	 
	except ValueError:
		keylist.sort()
		# No point in warning for override or All dictionaries
		if dname == param and not allFlag:
			print 'DB key %s not present in this dictionary. Going to be hard to insert. %s' % (dbkey, str(d))

	# So we're writing a  "Parameters" or "Override" dictionary (dname)...
	s.append('%s = {' % dname + os.linesep )
	s_aside = []
	for key in keylist:
		if key not in excl:
			comment = ''
			value = str(d[dname][key])
			# Sanitize the inputs (having some trouble with quotes being the contents of a field):
			value = value.strip("'")
			if value == None: value = ''
			# For each key and value, check for multiline values, and add triple quotes when necessary 
			if value.count(os.linesep):
				valsep = "'''"
			else:
				valsep = keysep
			# If the value is being set somewhere other than the config files, comment it and send it to the bottom of the list
			if dname == param and d.has_key(source) and d[source][key] is not 'Config':
				# Add a comment to the line with the provenance info 
				comment = ' # Defined in %s' % d[source][key]
				s_aside.append(spacing + keysep + key + keysep + dsep + valsep + value + valsep + pairsep + comment + os.linesep)
			else: s.append(spacing + keysep + key + keysep + dsep + valsep + value + valsep + pairsep + comment + os.linesep)
	# Add in all the commented fields
	s.insert(0,'\n')
	s.extend(s_aside)
	# Complete the dictionary
	s.append(spacing + '}' + os.linesep)
	s.append(os.linesep)
	return 0


def buildFile(name, d):
	'''Consolidate the composition and writing of the files'''
	startstr = '''
# This dictionary contains the parameters for one queue.
# Changing any parameter will update it in the schedconfig database.
# If you want to change a value temporarily, preserving the previous
# value, please put that parameter (key and new value) in the Override
# dictionary. Any value in Override will supersede any value in the
# Parameters dictionary.

# Parameters that have a comment appended ARE BEING SET ELSEWHERE!
# You can try to change them here, but it will FAIL!
# If you want to override one of these values, use the Override dictionary.

# Global values for entire sites can also be set
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

	switchstr = 'Enabled = '
	# Load up the file intro
	s=[startstr]
	# Put the queue on/off switch in place if not an All file
	if name is not All: s.append(switchstr + d[enab] + '\n\n')
	if name == All: allFlag = 1
	else: allFlag = 0
	# I'm taking advantage of the universality of lists.
	# composeFields is modifying the list itself rather than a copy.
	# Since the params in the All files have no keys to find, we warn
	# the composeFields code.
	composeFields(d, s, param, allFlag)
	s.append(overridestr)
	composeFields(d, s, over)
	
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

def buildDict():
	'''Build a copy of the queue dictionary from the configuration files '''

	confd={}
	# In executing files for variables, one has to put the variables in a contained, local context.
	locvars={}
	base = os.getcwd()
	# Loop throught the clouds in the base folder
	try:
		clouds = os.listdir(configs)
	except OSError:
		# If the configs folder is missing and this is the first thing run,
		# Reload this from the DB.
		# When SVN is in place, this should be replaced by a svn checkout.
		makeConfigs(sqlDictUnpacker(loadSchedConfig()))
		clouds = os.listdir(configs)

	for cloud in clouds:
		# Add each cloud to the dictionary
		confd[cloud] = {}
		# Loop throught the sites in the present cloud folder
		sites = os.listdir(configs + os.sep + cloud)
		for site in sites:
			# If this is the All file, create another entry.
			if site.endswith(postfix):
				# Get rid of the .py
				s=site.rstrip(postfix)
				# Run the file for the dictionaries
				fname = configs + os.sep + cloud + os.sep + site
				# The appropriate dictionaries are placed in locvars
				execfile(fname,{},locvars)
				confd[cloud][s][param] = locvars[param]
				confd[cloud][s][over] = locvars[over]
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
				# As a clarification, the Parameters, Override and Enabled variable are created when the config python file is executed
				fname = configs + os.sep + cloud + os.sep + site + os.sep + q
				# The appropriate dictionaries are placed in locvars
				execfile(fname,{},locvars)
				# Feed in the configuration
				confd[cloud][site][queue][param] = locvars[param]
				confd[cloud][site][queue][over] = locvars[over] 
				confd[cloud][site][queue][enab] = locvars[enab]
				confd[cloud][site][queue][source] = dict([(key,'Config') for key in locvars[param] if key not in excl]) 				
				
	# Leaving the All parameters unincorporated
	os.chdir(base)
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

# To be completed!! Needs to be part of a separate code file.
def jdlListAdder(d):
	'''Merge the contents of the jdllist table (name, host, system, jdltext) into the schedconfig dictionary'''
	pass


def makeConfigs(d):
	''' Reconstitutes the configuration files from the passed dictionary'''
	for cloud in d:
		# The working cloud path is made from the config base
		cloudpath = configs + os.sep + cloud
		# Create the config path for each site 
		for site in d[cloud]:
			# The working site path is made from the config base and cloud path
			path = cloudpath + os.sep + site
			# Try to make the path. If this fails, the path already exists.
			try:
				os.makedirs(path)
			except OSError:
				pass
			# Go on in...
			os.chdir(path)
			# And for each queue, create a config file. 
			for queue in d[cloud][site]:
				buildFile(queue, d[cloud][site][queue])
	return d

if __name__ == "__main__":
	#cloudd = sqlDictUnpacker(unPickler('pickledSchedConfig.p'))
	# Load the present status of the DB
	cloudd = sqlDictUnpacker(loadSchedConfig())
	configd = buildDict()
	status = allMaker(configd)
	
	# Compose the "All" queues for each site
	
	bdiiIntegrator(configd, cloudd)


	# Create the config path for each cloud


	os.chdir(base_path)
		
		
