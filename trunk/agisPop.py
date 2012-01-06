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
	elif cache == 'None':
		release = i['cache']
		project = i['cache']
	else:
		release = i['cache']
		project = i['cache']

	panda_resource = i['siteid']
	cmtconfig = i['cmtConfig']
	major_release = i['release']
	
	if releases.has_key(release):
		if releases[release]['cmtConfig'] == cmtconfig and releases[release]['project'] == project:
			pass
		elif not cmtconfig or not project:
			continue
		else:
			r=a.add_swrelease(release, cmtconfig=i['cmtConfig'], major_release=i['release'], project)
	r=a.add_panda_swrelease(panda_resource=i['siteid'], project, release, cmtconfig=i['cmtConfig'], major_release=i['release'])
