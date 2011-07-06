##########################################################################################
# Creation, reading and manipulation of the pandaconf access control list                #
#                                                                                        #
# Alden Stradling 3 Mar 2011                                                            #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

import os

from controllerSettings import *
from configFileHandling import *

#----------------------------------------------------------------------#
# Access File Handling
#----------------------------------------------------------------------

def readAccessControl(filename='/Users/stradlin/Code/SVNAdmin/pandaconf/conf/authz'):
	'''Read the present state of the authz file, parse out which sites and queues exist presently and which users are authorized'''

	# List positions of the various parts
	header = 0
	jdl = 1
	access = 2
	pathdef = 3
	trail = 4
	
	f=file(filename)
	s=f.read()
	f.close()
	
	# Flags have been added to the file as comments
	flaglist = ['# SITESPEC_HEADER_START', 
				'# JDL_START', # 0 Header -- stays unchanged
				'# ACCESSLIST_START', # 1 JDL -- gets filled with all the names we find
				'# PATHDEF_START', # 2 Access lists -- get updated with all the existing sites
				'# TRAILER_START', # 3 Path definitions -- link the permissions to SVN paths
				'# TRAILER_END'] # 4 Trailer -- stays unchanged

	parts=[]
	for i in range(len(flaglist) - 1):
		parts.append(s[s.find(flaglist[i]):s.find(flaglist[i+1])])
	parts.append(s[s.find(flaglist[i+1:])])
	


filename='/Users/stradlin/Code/SVNAdmin/pandaconf/conf/authz'
header = 0
jdl = 1
access = 2
pathdef = 3
trail = 4
comma = ','
eq = '='

f=file(filename)
s=f.read()
f.close()
	
# Flags have been added to the file as comments
flaglist = ['# SITESPEC_HEADER_START', 
			'# JDL_START', # 0 Header -- stays unchanged
			'# ACCESSLIST_START', # 1 JDL -- gets filled with all the names we find
			'# PATHDEF_START', # 2 Access lists -- get updated with all the existing sites
			'# TRAILER_START', # 3 Path definitions -- link the permissions to SVN paths
			'# TRAILER_END'] # 4 Trailer -- stays unchanged

parts=[]
cloud_d = {}
for i in range(len(flaglist) - 1):
	parts.append(s[s.find(flaglist[i]):s.find(flaglist[i+1])])
parts.append(s[s.find(flaglist[i+1]):])

cloud_l = parts[access].split(os.linesep+os.linesep)
cloud_l.pop(0)
for cloud in cloud_l:
	if cloud:
		cloudname = cloud.split(os.linesep)[0].split(eq)[0].strip()
		cloud_d[cloudname] = dict([(i.split(eq)[0].strip(),[j.strip() for j in i.split(eq)[1].strip().split(comma)]) for i in cloud.split(os.linesep) if i and i is not All])

usernames = {}
for cloud in cloud_d:
	for site in cloud_d[cloud]:
		usernames.update(dict([(i,0) for i in cloud_d[cloud][site] if i]))
usernames = sorted(usernames.keys())

confd = buildDict()
for cloud in confd:
	if cloud not in cloud_d: cloud_d[cloud] = {cloud:[]}
	for site in confd[cloud]:
		if site is not All and site is not svn:
			if site not in cloud_d[cloud]: cloud_d[cloud][site] = []

disable_d = dict([(i,{}) for i in cloud_d if i])
for cloud in cloud_d:
	if cloud not in confd: disable_d[cloud]=cloud_d[cloud].keys()
	else:
		for site in cloud_d[cloud]:
			if site not in confd[cloud]:
				disable_d[cloud]=cloud_d[cloud].keys()
				
			






#readAccessControl()
