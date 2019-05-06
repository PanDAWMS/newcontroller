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
    updStart = time.time()
    print("Collect list of PanDA Resources")
    pr_list = json.load(urllib.urlopen(agis_prlist_url))
    panda_resources = []
    for r in pr_list:
        if not r['panda_resource'] in panda_resources:
            panda_resources.append(r['panda_resource'])
    del pr_list
    pr_pos = 1
    delete_r = 0
    add_r = 0
    for pr in panda_resources:
        print("%s: %s from %s" % (pr, pr_pos, len(panda_resources)))
        print("Collect list if installed sw from PanDA DB for %s" % pr)
        sw_db = loadInstalledSW(pr)
        print("Got data from DB %s records" % len(sw_db))
        for i in sw_db:
            if not sw_db[i]['cmtConfig']:
                sw_db[i]['cmtConfig'] = 'None'
            if not sw_db[i]['cache']:
                sw_db[i]['cache'] = 'None'

        # Time to build the master list from AGIS:

        # The values will be de-duplicated in a dictionary. Keys will be (siteid,cloud,release,queue) together in a tuple
        # I'm not worried about redundant additions (the dictionary will handle that), but I _am_ concerned about
        # completeness. This is why I just add EVERYTHING and let the keys sort it out.

        sw_agis = {}

        print('Loading AGIS SW for %s' % pr)
        print(agis_sw_url_tmpl % pr)
        agisStart = time.time()
        agislist = json.load(urllib.urlopen(agis_sw_url_tmpl % pr))
        agisEnd = time.time()
        print('AGIS SW Load Time: %d [%s]' % ((agisEnd - agisStart), pr))
        agisStart = time.time()
        agissites = json.load(urllib.urlopen(agis_site_url_tmpl % pr))
        agisEnd = time.time()
        print 'AGIS site info Load Time: %d' % (agisEnd - agisStart)

        print('Build dictonary for update')
        for release in agislist:
            if not release['major_release']:
                continue
            if release['major_release'] != 'Conditions':
                # For the caches
                if release['major_release'] != release['release']:
                    index = '%s_%s_%s_%s' % (
                    release['panda_resource'], release['major_release'], release['project'] + '-' + release['release'],
                    release['cmtconfig'].replace('unset in BDII', ''))
                    unicodeEncode(index)
                    sw_agis[index] = {'siteid': release['panda_resource'], 'cloud': release['cloud'],
                                      'release': release['major_release'],
                                      'cache': release['project'] + '-' + release['release'],
                                      'cmtConfig': release['cmtconfig'].replace('unset in BDII', ''), 'validation': ''}
                    unicodeEncode(sw_agis[index])

                # For the releases
                else:
                    index = '%s_%s_%s_%s' % (release['panda_resource'], release['major_release'], 'None',
                                             release['cmtconfig'].replace('unset in BDII', ''))
                    unicodeEncode(index)
                    sw_agis[index] = {'siteid': release['panda_resource'], 'cloud': release['cloud'],
                                      'release': release['major_release'], 'cache': 'None',
                                      'cmtConfig': release['cmtconfig'].replace('unset in BDII', ''), 'validation': ''}
                    unicodeEncode(sw_agis[index])
            # Handling conditions correctly
            else:
                index = '%s_%s_%s_%s' % (release['panda_resource'], release['major_release'], 'None',
                                         release['cmtconfig'].replace('unset in BDII', ''))
                unicodeEncode(index)
                sw_agis[index] = {'siteid': release['panda_resource'], 'cloud': release['cloud'],
                                  'release': release['major_release'], 'cache': 'None',
                                  'cmtConfig': release['cmtconfig'].replace('unset in BDII', ''), 'validation': ''}
                unicodeEncode(sw_agis[index])
        # For CVMFS
        for site in agissites:
            if site['is_cvmfs']:
                unicodeEncode(index)
                index = '%s_%s_%s_%s' % (site['panda_resource'], 'CVMFS', 'None', 'None')
                sw_agis[index] = {'siteid': site['panda_resource'], 'cloud': site['cloud'], 'release': 'CVMFS',
                                  'cache': 'None', 'cmtConfig': 'None', 'validation': ''}
                unicodeEncode(sw_agis[index])

        del agislist
        del agissites
        #print('Convert to unicode')
        #unicodeEncode(sw_agis)
        #unicodeEncode(sw_db)

        # sw_union = sw_agis.copy()

        if os.environ.has_key('DBINTR'):
            setINTR = True
        else:
            setINTR = False
        # Debug mode for now on INTR
        print("Identify update/delete for %s" % pr)
        deleteList = [sw_db[i] for i in sw_db if i not in sw_agis]
        addList = [sw_agis[i] for i in sw_agis if i not in sw_db]

        for i in range(len(addList)):
            if not addList[i]['cmtConfig']: addList[i]['cmtConfig'] = 'None'
            if not addList[i]['cache']: addList[i]['cache'] = 'None'

        print("Add list contain: $s records"%(len(addList)))
        add_r += len(addList)
        for i in range(len(deleteList)):
            if not deleteList[i]['cmtConfig']: deleteList[i]['cmtConfig'] = 'None'
            if not deleteList[i]['cache']: deleteList[i]['cache'] = 'None'

        print("Del list contain: $s records"%(len(deleteList)))
        delete_r += len(deleteList)
        print('Update DB for %s' % pr)
        # Moved over to union of BDII and AGIS: seeing how it goes.
        try:
            updateInstalledSWdb(addList, deleteList)
        except:
            print('DB Update Failed for %s -- installedSW() (tried to add an existing row)' % pr)
        # print genDebug
        # if True:
        #	print 'Debug info for SW'
        #	return sw_db, sw_agis, deleteList, addList, sw_union
        pr_pos += 1

    updEnd = time.time()
    print 'Update took: %d' % (updEnd - updStart)
    print("%s records added" % add_r)
    print("%s records deleted" % delete_r)
    return 0
