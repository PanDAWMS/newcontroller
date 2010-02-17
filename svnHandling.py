#! /usr/bin/env python
#######################################################
# Handles changes to the configuration files and      #
# backup file stored in Subversion as a backup.       #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 17 Feb 10 #
#######################################################

import os, sys, commands, time

from controllerSettings import *

def svnCheckout():
	''' Check out the configuration SVN -- should only be necessary on rare occasions.'''
	path = os.getcwd()
	os.chdir(cfg_path)
	print '####### Checking out the SVN repository anew -- this should be a RARE event! Is this really what you want to do? #############'
	os.system('svn co %s' % configrepo)
	os.chdir(path)
	return 0

def svnUpdate(notestr = ''):
	''' Update the SVN repo with changes from BDII and ToA. Argument, if provided, notes which files received updates. '''
	path = os.getcwd()
	os.chdir(cfg_path)
	# Timestamp in GMT
	message = 'Changes made: %s%s' % (time.asctime(time.gmtime(),notestr))
	os.system('svn ci %s -m "%s"' % (configrepo, message))
	os.chdir(path)
	return 0
	
