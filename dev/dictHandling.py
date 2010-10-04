##########################################################################################
# Tools for handling dictionary operations in newController                              #
#                                                                                        #
# Alden Stradling 10 Oct 2009                                                            #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

import os, pickle

from miscUtils import *
from controllerSettings import *
from configFileHandling import *
from dbAccess import *
from svnHandling import *

#----------------------------------------------------------------------#
# Dictionary Handling
#----------------------------------------------------------------------#
def protoDict(queue,d,sourcestr='DB',keys=[]):
	'''Create a dictionary with params, overrides and sources for either an existing definition from the DB, or to add the dictionaries
	for a new queue. Used in sqlDictUnpacker for extraction of DB values (default) and in bdiiIntegrator for new queue addition from the BDII.'''
	en_val = 'True'
	if not len(d):
		en_val = 'False'
		d = {queue:dict([(key,None) for key in keys])}
	# Create a baseline queue definition to pass back, using list comprehensions, and with Source and Override empty dicts.
	return {param:d[queue].copy(),over:{},source:dict([(key,sourcestr) for key in d[queue].keys() if key not in excl]),enab:en_val}
	
def sqlDictUnpacker(d):
	'''Unpack the dictionary returned by Oracle or MySQL''' 
	# Organize by cloud, then site, then queue. Reading necessary data by key.
	# Remember that these vars are limited in scope.
	print 'Starting sqlDictUnpacker'
	cloud = 'cloud'
	site = 'site'
	out_d={}
	stdkeys = []
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
		# Append these new keys to standardkeys
		stdkeys.extend([key for key in d[queue].keys() if key not in excl])
	# Then remove all duplicates
	stdkeys=reducer(stdkeys)
	# Parse the dictionary to create an All queue for each site

	status = allMaker(out_d)
	# Take care of empty clouds (which are used to disable queues in schedconfig, for now) 
	# allMaker has to run before this to avoid causing KeyErrors with the new "empty cloud" values 
	if out_d.has_key(''):
		out_d[ndef]=out_d.pop('')
	if out_d.has_key(None):
		out_d[ndef]=out_d.pop(None)

	print 'Finishing sqlDictUnpacker'
	return out_d, stdkeys

def findQueue(q,d):
	'''Find a queue in the config dictionary and return the cloud and site. If not found, return empty values'''
	for cloud in d:
		for site in d[cloud]:
			# Not going to find sites in the All file
			if site == All:
				continue
			# Flip through all the queues and return if you find one.
			for queue in d[cloud][site]:
				if queue == q:
					return cloud, site
	# If nothing comes up, return empties.
	return '',''

def compareQueues(dbDict,cfgDict,dbOverride=False):
	'''Compares the queue dictionary that we got from the DB to the one in the config files. Any changed/deleted
	queues are passed back. If dbOverride is set true, the DB is used to modify the config files rather than
	the default. Queues deleted in the DB will not be deleted in the configs.'''
	updDict = {}
	delDict = {}
	unicodeConvert(dbDict)
	unicodeConvert(cfgDict)
	# Allow the reversal of master and subordinate dictionaries
	if dbOverride: dbDict, cfgDict = cfgDict, dbDict
	if dbDict == cfgDict: return updDict, delDict
	for i in dbDict:
		# If the configDict doesn't have the key found in the DB:
		if not cfgDict.has_key(i):
			# Queues must be deleted in the configs. The file has been removed, as long as we're not in DB override mode.
			if not dbOverride:
				# If the queue is not in the configs, delete it.
				delDict[i]=dbDict[i]
				continue
		# If the dictionaries don't match:
		if dbDict[i] != cfgDict[i]:
			cfgDict[i].update(dbDict[i].fromkeys([k for k in dbDict[i].keys() if not cfgDict.has_key(i)]))
			# If the queue was changed in the configs, tag it for update. In DB override, we aren't updating the DB.
			if not dbOverride and cfgDict.has_key(i): updDict[i]=cfgDict[i]
	# If the queue is brand new (created in a config file), it is added to update.
	for i in cfgDict:
		if not dbDict.has_key(i):
			# In DB override, we aren't updating the DB.
			if not dbOverride: updDict[i]=cfgDict[i]
	# Return the appropriate queues to update and eliminate
	return updDict, delDict

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
		# We choose element 0 to get the first result. This hack will go away.
		#configd = buildDict()
		#status = allMaker(configd)
		makeConfigs(sqlDictUnpacker(loadSchedConfig())[0])
		clouds = os.listdir(configs)
	if clouds.count('.svn') > 0: clouds.remove('.svn')
		
	for cloud in clouds:
		# Add each cloud to the dictionary
		confd[cloud] = {}
		# Loop throught the sites in the present cloud folder
		sites = os.listdir(configs + os.sep + cloud)
		for site in sites:
			# If this is the All file, create another entry.
			if site.endswith(postfix) and not site.startswith('.'):
				# Get rid of the .py
				s=site[:-len(postfix)]
				# Run the file for the dictionaries
				fname = configs + os.sep + cloud + os.sep + site
				# The appropriate dictionaries are placed in locvars
				try:
					execfile(fname,{},locvars)
				except SyntaxError:
					print 'Syntax error in file %s -- modifications not made to queue.' % fname
					continue
				confd[cloud][s][param] = locvars[param]
				confd[cloud][s][over] = locvars[over]
			# Add each site to the cloud
			confd[cloud][site] = {}
			# Loop throught the queues in the present site folders
			queues = [i for i in os.listdir(configs + os.sep + cloud + os.sep + site) if i.endswith(postfix) and not i.startswith('.')]
			for q in queues:
				# Remove the '.py' 
				queue=q[:-len(postfix)]
				# Add each queue to the site
				confd[cloud][site][queue] = {}
				# Run the file to extract the appropriate dictionaries
				# As a clarification, the Parameters, Override and Enabled variable are created when the config python file is executed
				fname = configs + os.sep + cloud + os.sep + site + os.sep + q
				# The appropriate dictionaries are placed in locvars
				try:
					execfile(fname,{},locvars)
				except SyntaxError:
					print 'Syntax error in file %s -- modifications not made to queue.' % fname
					continue
				# Feed in the configuration
				confd[cloud][site][queue][param] = locvars[param]
				confd[cloud][site][queue][over] = locvars[over] 
				try:
					if queue != All: confd[cloud][site][queue][enab] = locvars[enab]
				except KeyError:
					pass
				confd[cloud][site][queue][source] = dict([(key,'Config') for key in locvars[param] if key not in excl]) 				
				
	# Leaving the All parameters unincorporated
	os.chdir(base)
	unicodeConvert(confd)
	return confd

def collapseDict(d):
	'''Collapses a nested dictionary into a flat set of queues '''
	out_d = {}
	# Rip through the clouds
	for cloud in d:
		# And for each site
		for site in d[cloud]:
			# And for each queue
			for queue in d[cloud][site]:
				# Don't bother for an "All" queue yet -- see below.
				if queue == All or site == All: continue
				# If the queue is not Enabled, no need to work with it.
				if enab in d[cloud][site][queue]:
					if not d[cloud][site][queue][enab]: continue
				# Get the parameter dictionary (vs the source or the overrides).
				# This is a symbolic link, not a duplication:
				p = d[cloud][site][queue][param]
				# So copy out the values into a new dictionary (except excluded ones)
				out_d[queue] = dict([(key,p[key]) for key in p if key not in excl])
				# Make sure all the standard keys are present, even if not filled
				for key in standardkeys:
					if key not in p.keys(): out_d[queue][key] = None
					out_d[queue][key] = d[cloud][site][queue][over][key]
			# Now process the All entry for the site, if it exists
			if d[cloud][site].has_key(All):
				for queue in d[cloud][site]:
					# No point in trying to apply the All parameters to the All queue.
					if queue == All: continue
					# Get the parameter dictionary (vs the source or the overrides).
					# This is a link, not a duplication:
					allparams = d[cloud][site][All][param]
					# Add the queue overrides
					queueoverrides = d[cloud][site][queue][over]
					# Add the All overrides
					alloverrides = d[cloud][site][All][over]
					# So copy out the values into the present queue dictionary (except excluded ones)
					# Each copy process is independently error protected.
					# Site ALL parameters have first priority
					try:
						for key in [i for i in allparams if i not in excl]: out_d[queue][key] = allparams[key]
					except KeyError:
						pass
					# Followed by queue-specific overrides
					try:
						for key in [i for i in queueoverrides if i not in excl]: out_d[queue][key] = queueoverrides[key]
					except KeyError:
						pass
					# Followed by site-specific overrides
					try:
						for key in [i for i in alloverrides if i not in excl]: out_d[queue][key] = alloverrides[key]
					except KeyError:
						pass

	# Return the flattened dictionary
	for queue in out_d:
		# Sanitization.
		for key in out_d[queue]:
			# Checking for None and blank values as strings -- need to be NoneType for consistency
			if out_d[queue][key] == 'None' or out_d[queue][key] == '': out_d[queue][key] = None
			# Checking for ints and floats (consistency)
			if type(out_d[queue][key]) is str and out_d[queue][key].isdigit():
				try:
					out_d[queue][key] = int(out_d[queue][key])
				except ValueError:
					try:
						# Trying floats as well.
						out_d[queue][key] = float(out_d[queue][key])
					except ValueError:
						pass
		
	return out_d

def disabledQueues(d, key = param):
	''' Creates a list of dictionaries to be deleted because their Enabled state is False. Defaults to returning the params dict in the list. ''' 
	del_d = {}
	# Run through the clouds, sites and queues
	for cloud in d:
		for site in d[cloud]:
			for queue in d[cloud][site]:
				# If the queue has the Enabled flag (excluding All files):
				if enab in d[cloud][site][queue]:
					# If the flag is Enabled = False:
					if not d[cloud][site][queue][enab]:
						# Append that dictionary to the list
						del_d[queue] = d[cloud][site][queue][key]
	# And return the completed list to the main routine
	return del_d