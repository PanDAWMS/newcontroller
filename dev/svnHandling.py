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
	return 0
	if svnDebug: print 'Beginning SVN checkout'
	path = os.getcwd()
	try:
		os.chdir(cfg_path)
		os.chdir('..')
	except OSError:
		os.makedirs(cfg_path)
		os.chdir(cfg_path)
		os.chdir('..')
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
	# This needs to be made a lot smarter. FIX
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
	# Get the present path
	path = os.getcwd()
	try:
		# Go to the configs directory path, if possible
		os.chdir(cfg_path)
	except OSError:
		# Or create it if it's not there
		os.makedirs(cfg_path)
		os.chdir(cfg_path)
		os.chdir('..')
		#svnCheckout()
	# Update the whole subversion
	os.system('svn up')
	# Back to where you were.
	os.chdir(path)
	if svnDebug: print 'Completing SVN update'
	return 0
