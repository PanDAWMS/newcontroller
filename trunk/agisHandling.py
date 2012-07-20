# Put releases into AGIS

from agis.api.AGIS import AGIS
import pickle

a=AGIS(hostp='atlas-agis-api-dev.cern.ch:80')

def agisSiteMaxTime(sitename):
	'''Get the maxctime and maxcputime of the panda site and return the minimum of the two'''
	try:
		r=a.list_panda_queues(panda_site=sitename, extra_fields='queues')
		try:
			return min(int(r[sitename][0].queues[0]['ce_queue_maxwctime']),int(r[sitename][0].queues[0]['ce_queue_maxcputime']))
		except KeyError:
			print 'The AGIS instance lacks the maxtime values needed for this lookup for site %s' % sitename
	except:
		print 'Returning 0. Failed AGIS maxtime lookup for ' + sitename
		return 0








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
