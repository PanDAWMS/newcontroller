# Put releases into AGIS

from agis.api.AGIS import AGIS
import pickle

a=AGIS(hostp='atlas-agis-api-dev.cern.ch:80')
f=file('installedswdump.p')
l=pickle.load(f)
for i in l:
    try:
        r=a.add_swrelease(release=i['cache'].split('-')[1],cmtconfig=i['cmtConfig'],major_release=i['release'],project=i['cache'].split('-')[0])
    except:
        pass
    r=a.add_panda_swrelease(panda_resource=i['siteid'],project=i['cache'].split('-')[0], release=i['cache'].split('-')[1], cmtconfig=i['cmtConfig'],major_release=i['release'])
