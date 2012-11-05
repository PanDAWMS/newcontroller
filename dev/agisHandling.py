# Put releases into AGIS

from controllerSettings import *
import urllib
import cPickle, pickle
try:
	import json
except:
	import simplejson as json

agis_queues_url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=full'
agis_sw_url = 'http://atlas-agis-api.cern.ch/jsoncache/list_presource_sw.schedconf.json'

try:
	queueList = json.load(urllib.urlopen(agis_queues_url))
	softwareList = json.load(urllib.urlopen(agis_sw_url))
except IOError:
	f=file('queueList.p')
	queueList = cPickle.load(f)
	f.close()
	f=file('softwareList.p')
	queueList = cPickle.load(f)
	f.close()

def updateMaxTime(configd):
	'''Go through all the sites in a dictionary ordered by cloud and site, and update each queue with the appropriate maxtime value.'''
	queueDict = dict([(i['name'],i) for i in queueList])
	for cloud in [i for i in configd.keys() if (i != All and i != ndef and i not in ['TEST','OSG'])]:
		# For all regular sites:
		for site in [i for i in configd[cloud].keys() if (i != All and i != svn)]:
			# Loop over all the queues in the site, where the queue is not empty or "All"
			# Get the maxtime value for the site to pass to the queues.
			for queue in [i for i in configd[cloud][site].keys() if (i != All and i != svn)]:
				if queueDict.has_key(queue) and queueDict[queue].has_key('queues') and len(queueDict[queue]['queues'])\
					   and queueDict[queue]['queues'][0].has_key('ce_queue_maxcputime') \
					   and queueDict[queue]['queues'][0].has_key('ce_queue_maxwctime'):
					configd[cloud][site][queue][param]['maxtime'] = str(min(min(queueDict[queue]['queues'][0]['ce_queue_maxcputime'],queueDict[queue]['queues'][0]['ce_queue_maxwctime'])*60,maxMaxTime))
					configd[cloud][site][queue][source]['maxtime'] = 'AGIS'

## def resourceMap():
## 	'''Map all the Panda resources to CE endpoints'''
## 	tempList=[]
## 	for i in queueList:
		


tmp=[]
for i in queueList:
	tmp.append({'siteid':i['panda_resource'],'ce_endpoints':[q['ce_endpoint'].split(':')[0] for q in i['queues']],'ce_name':[q['ce_name'].split(':')[0] for q in i['queues']]})

def reducer(l):
	''' Reduce the entries in a list by removing dupes'''
	return dict([(i,1) for i in l]).keys()

tmp_nopo = {}
tmp_port = {}
for i in queueList:
	for q in i['queues']:
		if tmp_nopo.has_key(q['ce_endpoint'].split(':')[0]): tmp_nopo[q['ce_endpoint'].split(':')[0]].append(i['panda_resource'])
		else: tmp_nopo[q['ce_endpoint'].split(':')[0]] = [i['panda_resource']]

		if tmp_port.has_key(q['ce_endpoint']): tmp_port[q['ce_endpoint']].append(i['panda_resource'])
		else: tmp_port[q['ce_endpoint']] = [i['panda_resource']]

tmp=tmp_port
print len(tmp)
## for i in tmp:
## 	if len(reducer(tmp[i])) > 1:
## 		   print i, tmp[i]

tmp=tmp_nopo
print len(tmp)
## for i in tmp:
## 	if len(reducer(tmp[i])) > 1:
## 		   print i, tmp[i]











## def agisSiteMaxTime(sitename):
## 	'''Get the maxctime and maxcputime of the panda site and return the minimum of the two'''
## 	# This is in the dev instance only for now - change it over when the prod is working.
## 	queue_d={}
## 	try:
## 		r=a.list_panda_queues(panda_site=sitename, extra_fields='queues')
## 		for queue in r[sitename][0].queues:			
## 			try:
## 				# Convert to seconds! Respect the absolute max time.
## 				queue_d[queue.name] = min(min(int(queue['ce_queue_maxwctime']),int(r[sitename][0].queues[0]['ce_queue_maxcputime']))*60,maxMaxTime)
## 			except KeyError:
## 				if agisDebug: print 'The AGIS instance lacks the maxtime values needed for this lookup for site %s and queue %s' % (sitename, queue)
## 	except:
## 		if agisDebug: print 'Failed AGIS maxtime lookup for ' + sitename
## 		return -1


## import pickle
## f=file('/Users/stradlin/configd')
## configd = pickle.load(f)
## f.close()


## for cloud in [i for i in configd.keys() if (i != All and i != ndef and i not in ['TEST','OSG'])]:
## 	# For all regular sites:
## 	for site in [i for i in configd[cloud].keys() if (i != All and i != svn)]:
## 		# Loop over all the queues in the site, where the queue is not empty or "All"
## 		# Get the maxtime value for the site to pass to the queues.
## 		for queue in [i for i in configd[cloud][site].keys() if (i != All and i != svn)]:
## 			n += 1
## 			if queueDict.has_key(queue) and queueDict[queue].has_key('queues') and len(queueDict[queue]['queues'])\
## 				   and queueDict[queue]['queues'][0].has_key('ce_queue_maxcputime') \
## 				   and queueDict[queue]['queues'][0].has_key('ce_queue_maxwctime'):
## 				orig=configd[cloud][site][queue][param]['maxtime']
## 				configd[cloud][site][queue][param]['maxtime'] = str(min(min(queueDict[queue]['queues'][0]['ce_queue_maxcputime'],queueDict[queue]['queues'][0]['ce_queue_maxwctime'])*60,maxMaxTime))
## 				configd[cloud][site][queue][source]['maxtime'] = 'AGIS'
## 				if agisDebug: print  queue, orig, configd[cloud][site][queue][param]['maxtime'], queueDict[queue]['queues'][0]['ce_queue_maxcputime']*60, queueDict[queue]['queues'][0]['ce_queue_maxwctime']*60





## agis_url = 'http://atlas-agis-api-dev.cern.ch/request/pandaqueue/query/list/?json&preset=full'
## queueList = json.load(urllib.urlopen(agis_url))
## queueDict = dict([(i['name'],i) for i in queueList])
## n=0
## f=file('installedswdump.p')
## l=pickle.load(f)
## releases=a.list_swreleases()

## for i in l:
## 	if '-' in i['cache']:
## 		krelease = i['cache'].split('-')[1]
## 		kproject = i['cache'].split('-')[0]
## 	elif i['cache'] == 'None':
## 		krelease = i['cache']
## 		kproject = i['cache']
## 	else:
## 		krelease = i['cache']
## 		kproject = i['cache']

## 	kpanda_resource = i['siteid']
## 	kcmtconfig = i['cmtConfig']
## 	kmajor_release = i['release']

## 	add_flag = True
## 	if releases.has_key(krelease):
## 		for r in releases[krelease]:
## 			d=r.get_data()
## 			if d['cmtconfig'] == kcmtconfig and d['project'] == kproject:
## 				add_flag = False
## 		if add_flag:
## 			if not kcmtconfig or not kproject:
## 				continue

## 			try:
## 				print 'Adding %s' % kproject+krelease
## 				r=a.add_swrelease(release=krelease, cmtconfig=kcmtconfig, major_release=kmajor_release, project=kproject)
## 			except:
## 				pass
## 	try:
## 		print 'Adding site %s with release %s' % (krelease, kpanda_resource)
## 		r=a.add_panda_swrelease(panda_resource=kpanda_resource, project=kproject, release=krelease, cmtconfig=kcmtconfig, major_release=kmajor_release)
## 	except:
## 		print kpanda_resource, kproject, krelease, kcmtconfig, kmajor_release, r
