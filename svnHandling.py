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
	os.system('svn co  %s' % confrepo)
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
	for p in [hotBackupPath.split(os.sep)[-1], jdlconfigs.split(os.sep)[-1], configs.split(os.sep)[-1]]:
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
	os.system('svn update --accept theirs-full')
	try:
		# Go to the configs directory path, if possible
		os.chdir(cmp_path)
	except OSError:
		# Or create it if it's not there
		os.makedirs(cmp_path)
		os.chdir(cmp_path)
		os.chdir('..')
		#svnCheckout()
	# Update the whole subversion
	os.system('svn update --accept theirs-full')

	# Back to where you were.
	os.chdir(path)
	if svnDebug: 
		print 'Completing SVN update'
		return 0
	else:
		os.chdir(path)
		return 1

# Did it with shell script checking of time of day. Really much simpler. So this all is obsolete and nonfunctional. In for purely historical/pack-rat reasons

#def svnLock(update=False):
#	'''Shuts a lock file to the SVN for detection by the precommit script'''
#
#	# Go to the config files path
#	oldpath=os.getcwd()
#	os.chdir(cfg_path)
#	# Update the SVN before doing anything else, if required
#	if update: os.system('svn update --accept theirs-full')
#	# And check to be sure the lock file is not already locked
#	if commands.getoutput('grep Locked .lock.py'):
#		print 'Already locked'
#		os.chdir(oldpath)
#		return 1
#	# If all is well, copy the "Locked" version in as the lock.
#	# Echo wasn't working so well -- this was simple and efficient.
#	# Remember that this file has to be valid python to pass the other precommit filter
#	status = os.system('cp .locked.py .lock.py')
#	# If that went well, commit the change:
#	if not status and commands.getoutput('grep Locked .lock.py'):
#		status = os.system('svn ci .lock.py -m "Locked"')
#		# If all is still OK, return clean finish
#		if not status:
#			os.chdir(oldpath)
#			return 0
#	os.chdir(oldpath)
#	return 1
#
#def svnUnlock(commit=False):
#	'''Opens the lock file to the SVN for detection by the precommit script'''
#
#	# Go to the config files path
#	oldpath=os.getcwd()
#	os.chdir(cfg_path)
#	# Update the SVN before doing anything else
#	os.system('svn update --accept theirs-full')
#	# And check to be sure the lock file is not already locked
#	if commands.getoutput('grep Unlocked .lock.py'):
#		print 'Already unlocked'
#		os.chdir(oldpath)
#		return 1
#	# If all is well, copy the "Unlocked" version in as the lock.
#	# Echo wasn't working so well -- this was simple and efficient.
#	# Remember that this file has to be valid python to pass the other precommit filter
#	status = os.system('cp .unlocked.py .lock.py')
#	# If that went well, commit the change:
#	if not status and commands.getoutput('grep Unlocked .lock.py'):
#		# Reducing the commit time
#		if commit: status = os.system('svn ci .lock.py -m "Unlocked"')
#		# If all is still OK, return clean finish
#		if not status:
#			os.chdir(oldpath)
#			return 0
#	os.chdir(oldpath)
#	return 1
