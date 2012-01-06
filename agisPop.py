# Put releases into AGIS

from agis.api.AGIS import AGIS
import pickle

a=AGIS(hostp='atlas-agis-api-dev.cern.ch:80')
f=file('installedswdump.p')
l=pickle.load(f)
releases=a.list_swreleases()

for i in l:
	if '-' in i['cache']:
		release = i['cache'].split('-')[1]
		project = i['cache'].split('-')[0]
	elif i['cache'] == 'None':
		release = i['cache']
		project = i['cache']
	else:
		release = i['cache']
		project = i['cache']

	panda_resource = i['siteid']
	cmtconfig = i['cmtConfig']
	major_release = i['release']

	add_flag = True
	if releases.has_key(release):
		for r in releases[release]:
			d=r.get_data()
			if d['cmtconfig'] == cmtconfig and d['project'] == project:
				add_flag = False
		if add_flag:
			if not cmtconfig or not project:
				continue

			r=a.add_swrelease(release, cmtconfig, major_release, project)
	r=a.add_panda_swrelease(panda_resource, project, release, cmtconfig, major_release)
