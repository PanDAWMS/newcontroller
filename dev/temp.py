from dbAccess import *
import pickle
from newController import *

f=file('pickledlinfotool.p')
lcgdict=pickle.load(f)
f.close()

sw_db = loadInstalledSW()
release_tags = lcgdict.CEtags
cache_tags = lcgdict.CEctags
siteid = {}
gatekeeper = {}
cloud = {}
translateTags(cache_tags)
configd = buildDict()
confd=configd

for queue in confd:
	if confd[queue].has_key('siteid') and confd[queue]['siteid']:
		cloud[queue] = confd[queue]['cloud']
		siteid[queue] = confd[queue]['siteid']
		if confd[queue]['gatekeeper'] is not virtualQueueGatekeeper:
			gatekeeper[queue] = confd[queue]['gatekeeper']
		elif confd[queue]['queue']:
			gatekeeper[queue] = confd[queue]['queue'].split('/')[0]

sw_bdii = {}

for queue in siteid:
	if cache_tags.has_key(gatekeeper[queue]):
		for cache in cache_tags[gatekeeper[queue]]:
			release=baseReleaseSep.join(cache.split('-')[1].split(baseReleaseSep)[:nBaseReleaseSep])
			index = (siteid[queue],release,cache)
			sw_bdii[index] = {'siteid':siteid[queue],'cloud':cloud[queue],'release':release,'cache':cache}
	if release_tags.has_key(gatekeeper[queue]):
		for release in release_tags[gatekeeper[queue]]:
			index = (siteid[queue],release,None)
			sw_bdii[index] = {'siteid':siteid[queue],'cloud':cloud[queue],'release':release,'cache':None}



			
print 'Length of DB list: %d' % len(sw_db)
print 'Length of BDII list: %d' % len(sw_bdii)

deleteList = [sw_db[i] for i in sw_db if i not in sw_bdii]
addList = [sw_bdii[i] for i in sw_bdii if i not in sw_db]

print 'Length of deleteList: %d' % len(deleteList)
print 'Length of addList: %d' % len(addList)
