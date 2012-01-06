# Put releases into AGIS

from agis.api.AGIS import AGIS
import pickle

a=AGIS(hostp='atlas-agis-api-dev.cern.ch:80')
f=file('installedswdump.p')
l=pickle.load(f)
releases=a.list_swreleases()

for i in l:
	if '-' in i['cache']:
		krelease = i['cache'].split('-')[1]
		kproject = i['cache'].split('-')[0]
	elif i['cache'] == 'None':
		krelease = i['cache']
		kproject = i['cache']
	else:
		krelease = i['cache']
		kproject = i['cache']

	kpanda_resource = i['siteid']
	kcmtconfig = i['cmtConfig']
	kmajor_release = i['release']

	add_flag = True
	if releases.has_key(krelease):
		for r in releases[krelease]:
			d=r.get_data()
			if d['cmtconfig'] == kcmtconfig and d['project'] == kproject:
				add_flag = False
		if add_flag:
			if not kcmtconfig or not kproject:
				continue

			r=a.add_swrelease(release=krelease, cmtconfig=kcmtconfig, major_release=kmajor_release, project=kproject)
	r=a.add_panda_swrelease(panda_resource=kpanda_resource, project=kproject, release=krelease, cmtconfig=kcmtconfig, major_release=kmajor_release)
