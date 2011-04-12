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
		# Adapted to multi-cloud -- take the first as default.
		c=d[queue][cloud].split(',')[0]
		if c not in out_d:
			out_d[c] = {}
		# If the present queue's site isn't in the out_d cloud, create the site in the cloud.
		if d[queue][site] not in  out_d[c]:
			out_d[c][d[queue][site]] = {}

		# Once sure that we have all the cloud and site dictionaries created, we populate them with a parameter dictionary
		# an empty (for now) override dictionary, and a source dict. The override dictionary will become useful when we are reading back from
		# the config files we are creating. Each key is associated with a source tag -- Config, DB, BDII, ToA, Override, Site, or Cloud
		# That list comprehension at the end of the previous line just creates an empty dictionary and fills it with the keys from the queue
		# definition. The values are set to DB, and will be changed if another source modifies the value.
		out_d[c][d[queue][site]][d[queue][dbkey]] = protoDict(queue,d)
	
		# Model keyset for creation of queues from scratch
		# Append these new keys to standardkeys
		stdkeys.extend([key for key in d[queue].keys() if key not in excl])
	# Then remove all duplicates
	stdkeys=reducer(stdkeys)
	# Parse the dictionary to create an All queue for each site

	#status = allMaker(out_d)
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
		if i == All: continue
		if not dbDict.has_key(i):
			# In DB override, we aren't updating the DB.
			if not dbOverride: updDict[i]=cfgDict[i]
	# Return the appropriate queues to update and eliminate
	return updDict, delDict


def collapseDict(d):
	'''Collapses a nested dictionary into a flat set of queues '''
	out_d = {}
	# Rip through the clouds
	for cloud in d:
		# Exclude the queues in the NULL cloud, to prevent accidental collisions.
		if cloud == 'Deactivated':
			continue
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
				# If the value already exists, you are clobbering something -- it needs
				# to be reported
				if out_d.has_key(queue):
					print 'QUEUE NAME REDUNDANCY:'
					print p
					print 'Replacing'
					print out_d[queue]
				out_d[queue] = dict([(key,p[key]) for key in p if key not in excl])
				# Make sure all the standard keys are present, even if not filled
				for key in standardkeys:
					if key not in p.keys(): out_d[queue][key] = None
					# If there's a missing standard key in the overrides, let it on by
					try:
						out_d[queue][key] = d[cloud][site][queue][over][key]
					except KeyError:
						pass
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

def disabledQueues(d,dbd,key = param):
	''' Creates a list of dictionaries to be deleted because their Enabled state is False. Defaults to returning the params dict in the list.
	Check the db dictionary to see if the queue needs to be deleted.''' 
	del_d = {}
	# Run through the clouds, sites and queues
	for cloud in d:
		for site in d[cloud]:
			for queue in d[cloud][site]:
				# If the queue has the Enabled flag (excluding All files):
				if enab in d[cloud][site][queue]:
					# If the flag is Enabled = False and the queue is in the DB:
					if not d[cloud][site][queue][enab] and dbd[cloud][site].has_key(queue):
						# Append that dictionary to the list
						del_d[queue] = d[cloud][site][queue][key]						
	# And return the completed list to the main routine
	return del_d

def nicknameChecker(d):
	'''Checks through the DB to be sure that all queues have a nickname -- places the queue name in as nickname if not'''
	for cloud in d:
		for site in [i for i in d[cloud] if i != All]:			
			for queue in [i for i in d[cloud][site] if i != All]:
				if not d[cloud][site][queue][param].has_key(dbkey):
					d[cloud][site][queue][param][dbkey] = queue
	return 0
