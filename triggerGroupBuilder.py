import os, sys, commands
from miscUtils import reducer

groupName = 'atlpan:updatetrigger'

#configFile = '/afs/cern.ch/user/a/atlpan/admin/pandaconf/conf/authz'
configFile = '/Users/stradlin/Code/SVNAdmin/pandaconf/conf/authz'

pickupLines = ['developers','adc-users','librarian','JDL']
beginString = '# ACCESSLIST_START'
endString = '# PATHDEF_START'

f=file(configFile)
l=f.readlines()
f.close()
l=[i.strip() for i in l]
userList = [i.split('=')[1] for i in l if i.count('=') and i.split()[0] in pickupLines]
beginPosition = l.index(beginString) + 1
endPosition = l.index(endString)

for i in l[beginPosition:endPosition]:
	tmp = i.split(' = ')
	if len(tmp) > 1:
		userList.append(tmp[1])

users = set(sorted(reducer([i.strip() for i in ', '.join(userList).split(',')])))

members = commands.getoutput('pts membership %s' % groupName).split(os.linesep)[1:]
members = set(sorted([i.strip() for i in members]))
#members = set(['kreeves', 'poggio', 'xzhao', 'sgonzale', 'roball', 'serfon', 'aaron', 'wguan', 'stradlin', 'read', 'mdavid', 'dewhurst', 'jha', 'jwhuang', 'campanas', 'atlpan', 'rgardner', 'girolamo', 'severini', 'wdeng', 'rahal', 'jpardo', 'wwu', 'evamvako', 'dlesny', 'nikolici', 'mslater', 'bbockelm', 'gnegri', 'dvanders', 'douglas', 'jschovan', 'tshin', 'serpa', 'nathany', 'mcguigan', 'desalvo', 'ueda', 'suijian', 'lancone', 'jezequel', 'dgeerts', 'drebatto', 'iueda', 'walkerr', 'digirola', 'guirriec', 'ddmusr01', 'cranshaw', 'izen', 'nielsenj', 'aforti', 'espinal', 'sedov', 'ting', 'minaenko', 'alread', 'dmoore', 'libpanda', 'fbarreir', 'youssef', 'iourio', 'alexei', 'cwaldman', 'nilspal', 'jcaballe', 'buncic', 'vartap', 'smirnovi', 'hartem', 'graemes', 'iouri', 'grahal', 'scampana', 'desilva', 'sosebee', 'mckee', 'caronb', 'kaushik', 'pbuncic', 'harenber', 'tmaeno', 'helmut', 'hclee', 'yangw', 'pnilsson', 'psalgado', 'ekorolko', 'saewill', 'desalvzp', 'bdouglas', 'crepe', 'mpotekhi', 'rinaldil','adimu'])

adds = users - members
deletes = members - users

for i in adds:
	try:
		os.system('pts adduser %s %s' % (i,groupName))
		print 'Added user %s to group %s' % (i,groupName)
	except:
		pass

for i in deletes:
	try:
		os.system('pts removeuser %s %s' % (i,groupName))
		print 'Removed user %s from group %s' % (i,groupName)
	except:
		pass
