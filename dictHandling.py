##########################################################################################
# Tools for handling dictionary operations in newController                              #
#                                                                                        #
# Alden Stradling 10 Oct 2009                                                            #
# Alden.Stradling@cern.ch                                                                #
# 14 Feb 2012 - Queues deleted in the SVN are now not deleted in the DB                  #
##########################################################################################

import os, pickle, cPickle

from miscUtils import *
from controllerSettings import *
from dbAccess import *
from configFileHandling import *
#from svnHandling import *

import urllib

try:
	import json
except:
	import simplejson as json

#----------------------------------------------------------------------#
# Dictionary Handling
#----------------------------------------------------------------------#

def protoDict(queue,d,sourcestr='DB',keys=[]):
	'''Create a dictionary with params, overrides and sources for either an existing definition from the DB, or to add the dictionaries
	for a new queue. Used in sqlDictUnpacker for extraction of DB values (default) and in bdiiIntegrator for new queue addition from the BDII.'''
	en_val = 'True'
	if not len(keys):
		# Be sure there are only allowed keys
		keys = list(set(d[queue].keys()).difference(excl))
	if not len(d):
		en_val = 'False'
		d = {queue:dict([(key,None) for key in keys if key not in excl])}
	# Create a baseline queue definition to pass back, using list comprehensions, and with Source (default) and Override (empty) dicts.
	return {param:dict([(key,d[queue][key]) for key in keys if key not in excl and key in d[queue].keys()]),over:{},source:dict([(key,sourcestr) for key in keys if key not in excl and key in d[queue].keys()]),enab:en_val}
	
def agisDictUnpacker(standard_keys):
	'''Unpack the dictionary returned by AGIS for the sites''' 
	# Organize by cloud, then site, then queue. Reading necessary data by key.
	# Remember that these vars are limited in scope.
	# Take the standard keys from a census of the keys in the DB dictionary (dbd) in the main loop.
	print 'Starting agisDictUnpacker'

	try:
		d = json.load(urllib.urlopen(agis_queue_url))
	except IOError:
		f=file('queueList.p')
		d = cPickle.load(f)
		f.close()

	vo_name = 'vo_name'
	cloud = 'cloud'
	# This allows AGIS consistency, putting OSG and such in a different cloud field.
	site = 'site'
	out_d={}
	stdkeys = []
	# Run over the DB queues
	for queue in d:
		# If the present queue's cloud isn't in the out_d, create the cloud.
		# Adapted to multi-cloud -- take the first as default.
		# This is probably obsolete, but left in because it's harmless and who knows what people might try.
		c=d[queue][cloud].split(',')[0]
		if c not in out_d:
			out_d[c] = {}
		# If the present queue's site isn't in the out_d cloud, create the site in the cloud.
		if d[queue][site] not in  out_d[c]:
			out_d[c][d[queue][site]] = {}

		# Once sure that we have all the cloud and site dictionaries created, we populate them with a parameter dictionary
		# an empty (for now) override dictionary, and a source dict. The override dictionary will become useful when we are reading back from
		# the config files we are creating. (UPDATE: the oevrride is now obsolete as we move to AGIS) Each key is associated with a source tag
		##-- Config, DB, BDII, ToA, Override, Site, or Cloud (ALL NOW OBSOLETE. LEFT IN TO AVOID FRAGILE CODE)
		# That list comprehension at the end of the previous line just creates an empty dictionary and fills it with the keys from the queue
		# definition. The values are set to DB, and will be changed if another source modifies the value.
		out_d[c][d[queue][site]][d[queue][dbkey]] = protoDict(queue,d,'AGIS',standard_keys)
		# Filtering lists (unhashable and not DB compatible) and making them strings
        # Filtering dicts in the lists (unhashable and not DB compatible -- probably JSON, too) and attempting JSON. If it fails, stringify.
		# Checking for booleans that we need to convert to strings
		for key in out_d[c][d[queue][site]][d[queue][dbkey]][param]:
			if type(out_d[c][d[queue][site]][d[queue][dbkey]][param][key]) == list:
				try:
					out_d[c][d[queue][site]][d[queue][dbkey]][param][key] = '|'.join(out_d[c][d[queue][site]][d[queue][dbkey]][param][key])
				except TypeError:
					if type(out_d[c][d[queue][site]][d[queue][dbkey]][param][key][0]) == dict:
						try:
							out_d[c][d[queue][site]][d[queue][dbkey]][param][key] = json.dumps(out_d[c][d[queue][site]][d[queue][dbkey]][param][key])
						except:
							out_d[c][d[queue][site]][d[queue][dbkey]][param][key] = str(out_d[c][d[queue][site]][d[queue][dbkey]][param][key])
							print 'Nonfatal -- Queue %s, site %s tried to translate parameter %s from its detected "dict" format, and hit an exception. It has been expressed as a string.\n' % (queue, site, key)
							print 'Original: %s' % out_d[c][d[queue][site]][d[queue][dbkey]][param][key]
						# If the JSON converts but has no output (badly formatted), check for empties and warn.
						if out_d[c][d[queue][site]][d[queue][dbkey]][param][key] == '':
							print 'Nonfatal -- Queue %s, site %s tried to translate parameter %s from its detected "dict" format, and got an invalid JSON response (empty field).\n' % (queue, site, key)
							print 'Original: %s' % out_d[c][d[queue][site]][d[queue][dbkey]][param][key]                    
			if key in booleanStringFields:
				out_d[c][d[queue][site]][d[queue][dbkey]][param][key] = booleanStrings[out_d[c][d[queue][site]][d[queue][dbkey]][param][key]]
		# Fixing the "vo_name" to "cloud" disparity
		if d[queue][vo_name] != 'atlas':
			out_d[c][d[queue][site]][d[queue][dbkey]][param][cloud] = d[queue][vo_name].upper()
		else: out_d[c][d[queue][site]][d[queue][dbkey]][param][cloud] = d[queue][cloud]
	
	print 'Finishing agisDictUnpacker'
	return out_d

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

def compareQueues(dbDict,cfgDict):
	'''Compares the queue dictionary that we got from the DB to the one in the config files. Any changed
	queues are passed back.'''
	updDict = {}
	delDict = {}
	unicodeConvert(dbDict)
	unicodeConvert(cfgDict)
	for key in dbDict:
		# If the dictionaries don't match:
		if cfgDict.has_key(key):
			# Stringify the dictionaries and remove any excluded fields
			d = dict([(i,str(dbDict[key][i])) for i in dbDict[key] if i not in excl])
			c = dict([(i,str(cfgDict[key][i])) for i in cfgDict[key] if i not in excl])
			if c != d:
				cfgDict[key].update(dbDict[key].fromkeys([k for k in dbDict[key].keys() if not cfgDict.has_key(key)]))
				# If the queue was changed in the configs, tag it for update. In DB override, we aren't updating the DB.
				if cfgDict.has_key(key): updDict[key]=cfgDict[key]

	# If the queue is brand new in AGIS, it is added to update.
	for i in cfgDict:
		if i == All: continue
		if not dbDict.has_key(i):
			updDict[i] = cfgDict[i]

	# If the queue is gone from AGIS, put it in the deletes dictionary.
	for i in dbDict:
		if i == All: continue
		if not cfgDict.has_key(i):
			delDict[i] = dbDict[i].copy()

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

def keyCensus(d):
	'''Check the total list of keys used in the queues the collapsed dictionary d contains'''
	k = {}
	for i in d:
		k.update(dict(zip(d[i],[1 for i in range(len(d[i]))])))
	return set([i for i in k.keys() if i not in excl])

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
					try:
						if not d[cloud][site][queue][enab] and dbd[cloud][site].has_key(queue):
							# Append that dictionary to the list
							del_d[queue] = d[cloud][site][queue][key]
					except KeyError: # If the site or the cloud have been removed from the DB already
						pass
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

def BNL_ATLAS_1Deleter(d):
	'''For historical reasons, BNL_ATLAS_1 needs to never show up as a siteid or nickname. Checks through the DB to be sure that it does not'''
	deleteList=[]
	for cloud in d:
		for site in [i for i in d[cloud] if i != All]:			
			for queue in [i for i in d[cloud][site] if i != All]:
				if  d[cloud][site][queue][param]['siteid'].startswith('BNL_ATLAS_1') or d[cloud][site][queue][param][dbkey].startswith('BNL_ATLAS_1'):
					deleteList.append((cloud,site,queue))
	for cloud,site,queue in deleteList:
		status=d[cloud][site].pop(queue)
	return 0

def dd(d1, d2, ctx=""):
    print "Changes in " + ctx
    for k in d1:
        if k not in d2:
            print k + " removed from d2"
    for k in d2:
        if k not in d1:
            print k + " added in d2"
            continue
        if d2[k] != d1[k]:
            if type(d2[k]) not in (dict, list):
                print k + " changed in d2 to " + str(d2[k]) + ' from ' + str(d1[k])
            else:
                if type(d1[k]) != type(d2[k]):
                    print k + " changed to " + str(d2[k])  + ' from ' + str(d1[k])
                    continue
                else:
                    if type(d2[k]) == dict:
                        dd(d1[k], d2[k], k)
                        continue
    print "Done with changes in " + ctx
    return


