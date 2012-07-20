# Put releases into AGIS

from controllerSettings import *

from agis.api.AGIS import AGIS
import pickle
a=AGIS(hostp='atlas-agis-api-dev.cern.ch:80')

def agisSiteMaxTime(sitename):
	'''Get the maxctime and maxcputime of the panda site and return the minimum of the two'''
	# This is in the dev instance only for now - change it over when the prod is working.
	try:
		r=a.list_panda_queues(panda_site=sitename, extra_fields='queues')
		try:
			return min(int(r[sitename][0].queues[0]['ce_queue_maxwctime']),int(r[sitename][0].queues[0]['ce_queue_maxcputime']))
		except KeyError:
			if agisDebug: print 'The AGIS instance lacks the maxtime values needed for this lookup for site %s' % sitename
	except:
		if agisDebug: print 'Returning 0. Failed AGIS maxtime lookup for ' + sitename
		return -1


def updateSiteMaxTime(configd):
	'''Go through all the sites in a dictionary ordered by cloud and site, and update each queue with the appropriate maxtime value.'''
	for cloud in [i for i in configd.keys() if (i != All and i != ndef and i not in ['TEST','OSG'])]:
		# For all regular sites:
		for site in [i for i in configd[cloud].keys() if (i != All and i != svn)]:
			# Loop over all the queues in the site, where the queue is not empty or "All"
			# Get the maxtime value for the site to pass to the queues.
			t=agisSiteMaxTime(site)
			# Filter failed maxtime updates, which return -1, and zero values for maxtime
			if t > 0:
				for queue in [i for i in configd[cloud][site].keys() if (i != All and i != svn)]:
					configd[cloud][site][queue][param]['maxtime'] = str(t)






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
