#! /usr/bin/env python
#######################################################
# Handles backups as pickle files and such            #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 18 Feb 10 #
#######################################################

import os, sys, commands, time, pickle
from controllerSettings import *
from dictHandling import *

def backupCreate(d):
	''' Create a backup pickle file of a list of queue spec dictionaries that can be fed to direct DB updates''' 
	timestamp = '_'.join([str(i) for i in gmtime()[:-3]])
	# Check for an existing backup directory
	try:
		os.stat(backupPath)
	except:
		# And make one if not present
		os.makedirs(backupPath)
	# Opens the backup file and path
	f=file(backupPath + timestamp + backupName,'w')
	# Creates a list from the collapsed queue def dictionary and pickles it into the file
	pickle.dump([d[i] for i in collapseDict(d)],f)
	f.close()
	return 0

def backupRestore(fname):
	''' Restore a DB backup pickle into the live database! Caution! If you err, tou will hose the database! '''
	try:
		f=file(backupPath+fname)
	except IOError:
		print 'File %s is not found at %s. Exiting.' % (fname, backupPath)
		return 1
	try:
		l=pickle.load(f)
	except:
		print 'File %s%s not in backup/pickle format. Exiting.' % (fname, backupPath)
		return 1
	# Enable the DB
	utils.initDB()
	# Clear the DB
	utils.dictcursor().execute('DELETE FROM schedconfig')
	print 'Updating SchedConfig'
	#Insert the queues as per the backup
	utils.replaceDB('schedconfig',l,key=dbkey)
	# Commit both the deletion and the changes
	utils.commit()
	utils.endDB()
	return 0
