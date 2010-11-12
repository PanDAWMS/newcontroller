##########################################################################################
# Tools for handling installed software operations in newController                      #
#                                                                                        #
# Alden Stradling 21 Oct 2010                                                            #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

from SchedulerUtils import utils

from miscUtils import *
from controllerSettings import *
from dbAccess import *

def translateTags(d):
	'''Translate any legacy BDII tags and return new, clean lists. Assumes a dictionary of lists'''
	for key in d:
		# For each of the possible translations:
		for t in tagsTranslation:
			# For each gatekeeper, filter through the list and make any changes necessary.
			d[key] = [tag.replace(t,tagsTranslation[t]) for tag in d[key]]
	

def updateInstalledSW(confd,lcgdict):
	'''Checks for changes to the installedsw table, and add or delete releases as necessary by site'''
	# Call on the DB to get the present installedsw version. From dbAccess
	sw_db = loadInstalledSW()
	# Get the present BDII tags information from the (previously called) lcgInfositeTool2 
	release_tags = lcgdict.CEtags
	cache_tags = lcgdict.CEctags
	siteid = {}
	gatekeeper = {}
	cloud = {}
	# Make any translation necessary to the cache tags (see controllerSettings for more info)
	translateTags(cache_tags)

	# We now have a full set of lookups. We need to build a list of siteids, gatekeepers and clouds from the config dict:
	for queue in confd:
		# If the queue has a siteid, assign it and a gatekeeper. If !siteid, it's deactivated. 
		if confd[queue].has_key('siteid') and confd[queue]['siteid']:
			cloud[queue] = confd[queue]['cloud']
			siteid[queue] = confd[queue]['siteid']
			# If it's not an analysis queue and has a siteid, use gatekeeper as the BDII key
			if confd[queue]['gatekeeper'] != virtualQueueGatekeeper:
				gatekeeper[queue] = confd[queue]['gatekeeper']
			# If it's an analy queue, use the "queue" value instead
			elif confd[queue]['queue']:
				# and make sure you split off the non-gatekeeper-name part at the end.
				gatekeeper[queue] = confd[queue]['queue'].split('/')[0]
			# If there's no good gatekeeper information, forget the queue
			else:
				cloud.pop(queue)
				siteid.pop(queue)
	# Time to build the master list from BDII:

	# The values will be de-duplicated in a dictionary. Keys will be (siteid,release,queue) together in a tuple
	# I'm not worried about redundant additions (the dictionary will handle that), but I _am_ concerned about
	# completeness. This is why I just add EVERYTHING and let the keys sort it out.
	
	sw_bdii = {}
	
	for queue in siteid:
		# Check for the gatekeeper value in the BDII:
		if cache_tags.has_key(gatekeeper[queue]):
			for cache in cache_tags[gatekeeper[queue]]:
				# ASSUMPTION -- that base releases will always contain two periods as separators
				release=baseReleaseSep.join(cache.split('-')[1].split(baseReleaseSep)[:nBaseReleaseSep])
				# The unique name for this entry as a tuple
				index = '%s_%s_%s' % (siteid[queue],release,cache)
				sw_bdii[index] = {'siteid':siteid[queue],'cloud':cloud[queue],'release':release,'cache':cache}

		if release_tags.has_key(gatekeeper[queue]):
			for release in release_tags[gatekeeper[queue]]:
				cache = 'None'
				# The unique name for this entry as a tuple
				index = '%s_%s_%s' % (siteid[queue],release,cache)
				sw_bdii[index] = {'siteid':siteid[queue],'cloud':cloud[queue],'release':release,'cache':None}
	
	unicodeEncode(sw_bdii)
	unicodeEncode(sw_db)

	deleteList = [sw_db[i] for i in sw_db if i not in sw_bdii]
	addList = [sw_bdii[i] for i in sw_bdii if i not in sw_db]
	
	try:
		updateInstalledSWdb(addList,deleteList)
	except:
		print 'DB Update Failed -- installedSW() (tried to add an existing row)'
	if genDebug: return sw_db, sw_bdii, deleteList, addList, deleteDB, addDB, confd, cloud, siteid, gatekeeper  
