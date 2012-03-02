########################################
# Check for differences in the SVN and a reference copy.

import os, sys, random
from controllerSettings import *

def checkConfigs(attempt = 0)
	cfg_list, cmp_list = [], []

	# Get the file trees except the subversion stuff
	for dirname, dirnames, filenames in os.walk(cfg_path):
		for filename in filenames:
			if '.svn' not in dirname: cfg_list.append(os.path.join(dirname, filename))

	for dirname, dirnames, filenames in os.walk(cmp_path):
		for filename in filenames:
			if '.svn' not in dirname: cmp_list.append(os.path.join(dirname, filename).replace(cmp_path,cfg_path))

	# Compare the lists.
	diff = list(set(cfg_list).difference(set(cmp_list)))
	# If they differ, 
	if len(diff) and len(cmp_list) > 10:
		os.system('mv %s %s_aside_%d' % (cfg_path, cfg_path, random.randint(0,10000)))
		os.system('mv %s %s' % (cmp_path, cfg_path))
		os.system('svn co %s %s' % (confrepo, cmp_path))
		if attempt < 5:
			status = checkConfigs(attempt + 1)
		else:
			return -1
		
