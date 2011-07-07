##########################################################################################
# Creation, reading and manipulation of the pandaconf access control list                #
#                                                                                        #
# Alden Stradling 3 Mar 2011                                                            #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

import os

from controllerSettings import *
from configFileHandling import *
from miscUtils import *

#----------------------------------------------------------------------#
# Access File Handling
#----------------------------------------------------------------------

def readAccessControl(filename='/data/atlpan/panda/prod/SVNAdmin/pandaconf/conf/authz'):
	'''Read the present state of the authz file, parse out which sites and queues exist presently and which users are authorized'''

	# This will eventually be modified to be a command line parameter to pass for any given user.
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
	flaglist = ['# SITESPEC_HEADER_START', # 0 Header -- stays unchanged
				'# JDL_START', # 1 JDL -- gets filled with all the names we find
				'# ACCESSLIST_START', # 2 Access lists -- get updated with all the existing sites 
				'# PATHDEF_START', # 3 Path definitions -- link the permissions to SVN paths
				'# TRAILER_START', # 4 Trailer -- stays unchanged 
				'# TRAILER_END'] 

	parts=[]
	cloud_d = {}

	# Split the authz file on the flags, add to a list
	for i in range(len(flaglist) - 1):
		parts.append(s[s.find(flaglist[i]):s.find(flaglist[i+1])])
	parts.append(s[s.find(flaglist[i+1]):])

	# Get the JDL definition, and split it on the commas
	jdl_l = parts[1].split(os.linesep)[1].split('=')[1].split(',')
	jdl_l = [i.strip() for i in jdl_l]

	# Split the Access Control section by cloud, then site.
	cloud_l = parts[access].split(os.linesep+os.linesep)
	# Clear off the tag
	cloud_l.pop(0)
	for cloud in cloud_l:
		if cloud:
			# Separate the name into a key
			cloudname = cloud.split(os.linesep)[0].split(eq)[0].strip()
			# and make a dictionary of the key. The sites are subdictionary keys, and the username lists are each associated with a site. If the site is All, for some reason, exclude. 
			cloud_d[cloudname] = dict([(i.split(eq)[0].strip(),[j.strip() for j in i.split(eq)[1].strip().split(comma)]) for i in cloud.split(os.linesep) if i and i is not All])

	# We're going to aggregrate usernames in a dictionary to avoid duplication, and then give them all access to the JDLs
	usernames = {}
	for cloud in cloud_d:
		for site in cloud_d[cloud]:
			usernames.update(dict([(i,0) for i in cloud_d[cloud][site] if i]))
	# This is what we'll merge with the JDL list.
	usernames.update(dict([(i,0) for i in jdl_l if i]))

	# This is the final form of the JDL list, and we'll use it to populate the new file.
	jdl_l = sorted(usernames.keys())

	# Now we check the new config files, seeing if there are sites and clouds to add or remove
	confd = buildDict()
	for cloud in confd:
		if cloud not in cloud_d: cloud_d[cloud] = {cloud:[]}
		for site in confd[cloud]:
			if site is not All and site != svn:
				if site not in cloud_d[cloud]: cloud_d[cloud][site] = []

	# Removing obsolete clouds and sites ... and dumping their users. They can be re-added later, if necessary, from SVN versions.
	killcloud = []
	killsite = []

	# Add clouds and sites slated for removal to a kill list (on the fly killing changes the dict length, and causes trouble)
	for cloud in cloud_d:
		if cloud not in confd: killcloud.append(cloud)
		else:
			for site in cloud_d[cloud]:
				if site not in confd[cloud] and site != cloud: killsite.append((cloud, site))

	# Create a record wherein we can dump obsolete users for JDL reasons
	dumpusers = []
	# We'll check later to see if they survived elsewhere

	# Wipe the obsolete clouds
	for cloud in killcloud:
		dump = cloud_d.pop(cloud) 
		for site in dump: dumpusers.extend(dump[site])

	# Wipe the obsolete sites
	for cloud, site in killsite:
		dump = cloud_d[cloud].pop(site)
		dumpusers.extend(dump)

	# Now rebuild the site and cloud list. Replace the access control and path definition sections completely.

	# Add the access header
	parts[access] = flaglist[access] + os.linesep

	# Create the access lists
	for cloud in cloud_d:
		# Create the general cloud access line
		parts[access] += cloud + ' = ' + ', '.join(cloud_d[cloud].pop(cloud)) + os.linesep
		for site in cloud_d[cloud]:
			parts[access] += site + ' = ' + ', '.join(cloud_d[cloud][site]) + os.linesep
		# Final linefeed to separate clouds
		parts[access] += os.linesep

	# Add the pathdef header
	parts[pathdef] = flaglist[pathdef] + os.linesep + '[/trunk/JDLConfigs]\n@JDL = rw' + os.linesep + os.linesep

	# Create the access paths
	for cloud in cloud_d:
		# Create the general cloud access line
		parts[pathdef] += '[/trunk/SchedConfigs/%s]' % cloud + os.linesep
		parts[pathdef] += '@%s = rw' % cloud + os.linesep
		for site in cloud_d[cloud]:
			parts[pathdef] += '[/trunk/SchedConfigs/%s/%s]' % (cloud,site) + os.linesep
			parts[pathdef] += '@%s = rw' % site + os.linesep
		# Final linefeed to separate clouds
		parts[pathdef] += os.linesep

	# Create the new JDL list

	# Check to be sure (as mentioned above) that removed users really are obsolete
	users = []
	for cloud in cloud_d:
		for site in cloud_d[cloud]: users.extend(cloud_d[cloud][site])

	users = reducer(users)

	redeem = []
	for user in dumpusers:
		if user in users: redeem.append(user)

	for user in jdl_l:
		if user not in users: dumpusers.append(user)

	# Remove duplicates
	redeem = reducer(redeem)
	if '' in redeem: redeem.remove('')
	dumpusers = reducer(dumpusers)
	if '' in dumpusers: dumpusers.remove('')

	# If the user has been redeemed, leave him on the JDL list
	for user in redeem: dumpusers.remove(user)
	# Otherwise, delete him
	for user in dumpusers:				
		try:
			jdl_l.remove(user)
		except:
			pass

	# Add the JDL header
	parts[jdl] = flaglist[jdl] + os.linesep + 'JDL = '
	# Compose the JDL permissions
	parts[jdl] += ', '.join(jdl_l) + os.linesep
	# Final linefeed to separate clouds
	parts[jdl] += os.linesep

	f=file(filename,'w')
	f.write(''.join(parts))
	f.close()



