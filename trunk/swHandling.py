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
import urllib, time
try:
	import json
except:
	import simplejson as json

def updateInstalledSW(confd):
	'''Checks for changes to the installedsw table, and add or delete releases as necessary by site'''
	# Call on the DB to get the present installedsw version. From dbAccess
	sw_db = loadInstalledSW()
	for i sw_db:
		if not sw_db[i]['cmtConfig']: sw_db[i]['cmtConfig'] = 'None'
		if not sw_db[i]['cache']: sw_db[i]['cache'] = 'None'

	# Time to build the master list from AGIS:

	# The values will be de-duplicated in a dictionary. Keys will be (siteid,cloud,release,queue) together in a tuple
	# I'm not worried about redundant additions (the dictionary will handle that), but I _am_ concerned about
	# completeness. This is why I just add EVERYTHING and let the keys sort it out.
	
	sw_agis = {}

	print 'Loading AGIS SW'
	agisStart = time.time()
	agislist = json.load(urllib.urlopen(agis_sw_url))
	agisEnd = time.time()
	print 'AGIS SW Load Time: %d' % (agisEnd - agisStart)
	agisStart = time.time()
	agissites = json.load(urllib.urlopen(agis_site_url))
	agisEnd = time.time()
	print 'AGIS site info Load Time: %d' % (agisEnd - agisStart)

	for release in agislist:
		if not release['major_release']: continue
		if release['major_release'] != 'Conditions':
			# For the caches
			if release['major_release'] != release['release']:
				index = '%s_%s_%s_%s' % (release['panda_resource'],release['major_release'],release['project']+'-'+release['release'],release['cmtconfig'].replace('unset in BDII',''))
				sw_agis[index] = {'siteid':release['panda_resource'],'cloud':release['cloud'],'release':release['major_release'],'cache':release['project']+'-'+release['release'],'cmtConfig':release['cmtconfig'].replace('unset in BDII',''),'validation':''}

			# For the releases
			else:
				index = '%s_%s_%s_%s' % (release['panda_resource'],release['major_release'],'None',release['cmtconfig'].replace('unset in BDII',''))
				sw_agis[index] = {'siteid':release['panda_resource'],'cloud':release['cloud'],'release':release['major_release'],'cache':'None','cmtConfig':release['cmtconfig'].replace('unset in BDII',''),'validation':''}

		# Handling conditions correctly
		else:
			index = '%s_%s_%s_%s' % (release['panda_resource'],release['major_release'],'None',release['cmtconfig'].replace('unset in BDII',''))
			sw_agis[index] = {'siteid':release['panda_resource'],'cloud':release['cloud'],'release':release['major_release'],'cache':'None','cmtConfig':release['cmtconfig'].replace('unset in BDII',''),'validation':''}
			
	# For CVMFS
	for site in agissites:
		if site['is_cvmfs']:
			index = '%s_%s_%s_%s' % (site['panda_resource'],'CVMFS','None','None')
			sw_agis[index] = {'siteid':site['panda_resource'],'cloud':site['cloud'],'release':'CVMFS','cache':'None','cmtConfig':'None','validation':''}

	unicodeEncode(sw_agis)
	unicodeEncode(sw_db)

	sw_union = sw_agis.copy()

	if os.environ.has_key('DBINTR'): setINTR = True
	else: setINTR = False
	# Debug mode for now on INTR
	
	deleteList = [sw_db[i] for i in sw_db if i not in sw_union]
	addList = [sw_union[i] for i in sw_union if i not in sw_db]

	for i in range(len(addList)):
		if not addList[i]['cmtConfig']: addList[i]['cmtConfig'] = 'None'
		if not addList[i]['cache']: addList[i]['cache'] = 'None'

	for i in range(len(deleteList)):
		if not deleteList[i]['cmtConfig']: deleteList[i]['cmtConfig'] = 'None'
		if not deleteList[i]['cache']: deleteList[i]['cache'] = 'None'

	# Moved over to union of BDII and AGIS: seeing how it goes.
	try:
		updateInstalledSWdb(addList,deleteList)
	except:
		print 'DB Update Failed -- installedSW() (tried to add an existing row)'
	print genDebug
	if True:
		print 'Debug info for SW'
		return sw_db, sw_agis, deleteList, addList, confd, sw_union
	else:
		return 0
