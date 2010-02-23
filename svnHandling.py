#! /usr/bin/env python
#######################################################
# Handles changes to the configuration files and      #
# backup file stored in Subversion as a backup.       #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 17 Feb 10 #
#######################################################

# Might be nice to add some safeties -- checks on the success of these operations.

import os, sys, commands, time

from controllerSettings import *

def svnCheckout():
	''' Check out the configuration SVN -- should only be necessary on rare occasions.'''
	if svnDebug: print 'Beginning SVN checkout'
	path = os.getcwd()
	try:
		os.chdir(cfg_path)
	except OSError:
		os.makedirs(cfg_path)
		os.chdir(cfg_path)
	print '####### Checking out the SVN repository anew -- this should be a RARE event! Is this really what you want to do? #############'
	# Check out the whole repo
	os.system('svn co %s' % confrepo)
	os.chdir(path)
	if svnDebug: print 'Completing SVN checkout'
	return 0

def svnCheckin(notestr = ''):
	''' Update the SVN repo with changes from BDII and ToA. Argument, if provided, notes which files received updates. '''
	if svnDebug: print 'Beginning SVN checkin'
	path = os.getcwd()
	os.chdir(cfg_path)
	# Timestamp in GMT
	message = 'Changes made: %s%s' % (time.asctime(time.gmtime()),notestr)
	# Add all new files before checking in
	for p in [backupPath.split(os.sep)[-1], jdlconfigs.split(os.sep)[-1], configs.split(os.sep)[-1]]:
		o=commands.getoutput('svn add %s/*' % p)
	if svnDebug: print o
	o=commands.getoutput('svn add %s/*/*' % p)
	if svnDebug: print o
	o=commands.getoutput('svn add %s/*/*/*' % p)
	if svnDebug: print o
	# Check in the subversion
	os.system('svn ci -m "%s"' % (message))
	# Go back to original path
	os.chdir(path)
	if svnDebug: print 'Completing SVN checkin'
	return 0

def svnUpdate():
	''' Update from the SVN repo -- introduce changes from user configs. '''
	if svnDebug: print 'Beginning SVN update'
	path = os.getcwd()
	try:
		os.chdir(cfg_path)
	except OSError:
		svnCheckout()
	# Update the whole subversion
	os.system('svn up')
	os.chdir(path)
	if svnDebug: print 'Completing SVN update'
	return 0

