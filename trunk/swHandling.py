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
import urllib
try:
	import json
except:
	import simplejson as json


def translateTags(d):
	'''Translate any legacy BDII tags and return new, clean lists. Assumes a dictionary of lists'''
	for key in d:
		# For each of the possible translations:
		for t in tagsTranslation:
			# For each gatekeeper, filter through the list and make any changes necessary. Release records are (release,cmt)
			d[key] = [(d[key][tag][rel].replace(t,tagsTranslation[t]),d[key][tag][cmt]) for tag in range(len(d[key]))]
	
def fixCMT(tags):
	'''Fix any prepended stuff hanging off of the CE tags or ctags that was added by careless BDII population'''
	for site in tags:
		newSite = []
		for tag in tags[site]:
			if tag[cmt].count('-') > cmtDashes:
				cmt_spec = '-'.join(tag[cmt].split('-')[-(cmtDashes + 1):])
				rel_spec = tag[rel] + '-' + '-'.join(tag[cmt].split('-')[:(tag[cmt].count('-')-cmtDashes)])
				if swDebug: print rel_spec, cmt_spec
				newSite.append((rel_spec, cmt_spec))
			elif tag[cmt].count('-') < cmtDashes and len(tag[cmt]):
				if swDebug: print 'Skipping ', str(tag)
				pass
			else: newSite.append(tag)
		tags[site] = newSite
	return 0

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
	fixCMT(release_tags)
	fixCMT(cache_tags)
	# We now have a full set of lookups. We need to build a list of siteids, gatekeepers and clouds from the config dict:
	for queue in confd:
		# If the queue has a siteid, assign it and a gatekeeper. If !siteid, it's deactivated. 
		if confd[queue].has_key('siteid') and confd[queue]['siteid']:
			# Collect the cloud information, choosing only principal cloud. Comma-delimited.
			cloud[queue] = confd[queue]['cloud'].split(',')[0]
			siteid[queue] = confd[queue]['siteid']
			if len(cloud[queue]) > 8: print cloud[queue], queue, siteid[queue]
			# If it's not an analysis queue and has a siteid, use gatekeeper as the BDII key
			if confd[queue]['gatekeeper'] and confd[queue]['gatekeeper'] != virtualQueueGatekeeper:
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

	# The values will be de-duplicated in a dictionary. Keys will be (siteid,cloud,release,queue) together in a tuple
	# I'm not worried about redundant additions (the dictionary will handle that), but I _am_ concerned about
	# completeness. This is why I just add EVERYTHING and let the keys sort it out.
	
	sw_bdii = {}
	sw_agis = {}

	print 'Loading AGIS SW'
	agislist = []# json.load(urllib.urlopen(agisurl))

	for release in agislist:
		# For the caches
		if release['major_release'] != release['release']:
			index = '%s_%s_%s_%s' % (release['panda_resource'],release['major_release'],release['project']+'-'+release['release'],release['cmtconfig'].replace('unset in BDII',''))
			sw_agis[index] = {'siteid':release['panda_resource'],'cloud':'','release':release['major_release'],'cache':release['project']+'-'+release['release'],'cmtConfig':release['cmtconfig'].replace('unset in BDII','')}

		# For the releases
		index = '%s_%s_%s_%s' % (release['panda_resource'],release['project']+'-'+release['major_release'],'',release['cmtconfig'])
		sw_agis[index] = {'siteid':release['panda_resource'],'cloud':'','release':release['project']+'-'+release['major_release'],'cache':'None','cmtConfig':release['cmtconfig']}
		
	for queue in siteid:
		# Check for the gatekeeper value in the BDII:
		if cache_tags.has_key(gatekeeper[queue]):
			for cache in cache_tags[gatekeeper[queue]]:
				# Once again -- cache[rel] is cache[0], because the release and cache records are (release,cmt)
				# ASSUMPTION -- that base releases will always contain two periods as separators
				release=baseReleaseSep.join(cache[rel].split('-')[1].split(baseReleaseSep)[:nBaseReleaseSep])
				if '-' in release: print release, cache, cache[rel].split('-')[1].split(baseReleaseSep)[:nBaseReleaseSep]
				# The unique name for this entry				
				index = '%s_%s_%s_%s' % (siteid[queue],release,cache[rel],cache[cmt])
				index = index.replace('None','')
				sw_bdii[index] = {'siteid':siteid[queue],'cloud':cloud[queue],'release':release,'cache':cache[rel],'cmtConfig':cache[cmt]}

		if release_tags.has_key(gatekeeper[queue]):
			for release in release_tags[gatekeeper[queue]]:
				cache = 'None'
				# The unique name for this entry
				index = '%s_%s_%s_%s' % (siteid[queue],release[rel],cache,release[cmt])
				index = index.replace('None','')
				sw_bdii[index] = {'siteid':siteid[queue],'cloud':cloud[queue],'release':release[rel],'cache':cache,'cmtConfig':release[cmt]}
	

	unicodeEncode(sw_bdii)
	unicodeEncode(sw_agis)
	unicodeEncode(sw_db)

	uniqueAGIS = set(sw_agis.keys()) - set(sw_bdii.keys())
	uniqueBDII = set(sw_bdii.keys()) - set(sw_agis.keys())
	
	deleteList = [sw_db[i] for i in sw_db if i not in sw_bdii]
	addList = [sw_bdii[i] for i in sw_bdii if i not in sw_db]
	
	try:
		updateInstalledSWdb(addList,deleteList)
	except:
		print 'DB Update Failed -- installedSW() (tried to add an existing row)'
	print genDebug
	if True:
		print 'Debug info for SW'
		return sw_db, sw_bdii, deleteList, addList, confd, cloud, siteid, gatekeeper, uniqueBDII, uniqueAGIS  
	else:
		return 0
