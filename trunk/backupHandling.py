#! /usr/bin/env python
#######################################################
# Handles backups as pickle files and such            #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 18 Feb 10 #
#######################################################

import os, sys, commands, time, pickle, gzip, csv
from controllerSettings import *
from dictHandling import *
from dbAccess import *


def checkPreviousVolatiles():
	''' Tabulate information on previous volatile backups. Compare the recent trend to any present queues that will be created.
	If the queue was in recent existence, restore the volatiles for it as well.'''
	# Check for all the CSV files created recently, and select the most recent 10.
	l = [i for i in sorted(os.listdir(backupPath)) if volatileCSVName in i][:10]
	

def volatileBackupCreate():
	'''Back up the volatile parts of the DB before every operation'''
	# Get the present status of schedconfig
	d=loadSchedConfig()
	try:
		os.stat(backupPath)
	except:
		# And make one if not present
		os.makedirs(backupPath)
	# Grab the backup's timestamp
	timestamp = '_'.join([str(i).zfill(2) for i in time.gmtime()[:-3]])+'_'
	# Create the backup file as a gzip file for space reasons
	bfile = backupPath + timestamp + volatileSQLName + '.gz'
	f=gzip.open(bfile,'w')
	# The interesting fields are the ones specifically excluded from schedconfig backup
	fields = ['nickname'] + excl_nonTimestamp
	# For each of the queues, create a SQL restore command. The atlas_pandameta is unnecessary for restoration using the volatileRestore.py script, ans is stripped
	# off there... but this format allows direct copy-paste into SQL Developer or another direct SQL tool as well. This can be handy for manual or custom restores.
	# The Site and Cloud comments help filter the lines to allow manual restoration of specific sites and clouds using grep, avoiding a broad-brush approach.
	for i in d:
		f.write('UPDATE atlas_pandameta.schedconfig set %s WHERE nickname = \'%s\'; -- Site is %s, Cloud is %s\n' % (', '.join(['%s = \'%s\'' % (key, d[i][key]) for key in excl_nonTimestamp]), i, d[i]['site'],d[i]['cloud']))
	f.close()
	# Here we create a CSV file that allows a history of the volatiles to be kept. When a queue is being created, we have to be sure it's not a recent delete.
	# When a queue is reconstituted, this will allow a check for that queue nickname in the last few updates... and if it's present, we take the newest parameters
	# and restore them.
	bfile = backupPath + timestamp + volatileCSVName + '.gz'
	# Open the file via gzip
	f = gzip.open(bfile,'w')
	# Attach a CSV writer to the file
	w = csv.writer(f)
	# Write the header row
	w.writerow(fields)
	# Write all relevant volatile data
	for i in d:
		w.writerow([d[i][key] for key in  fields])
	f.close()
	return 0

def backupCreate(d):
	''' Create a backup pickle file of a list of queue spec dictionaries that can be fed to direct DB updates''' 
	if pickleDebug: print 'Starting pickle creation'
	timestamp = '_'.join([str(i).zfill(2) for i in time.gmtime()[:-3]])+'_'
	# Check for an existing backup directory
	try:
		os.stat(backupPath)
	except:
		# And make one if not present
		os.makedirs(backupPath)
	# Opens the backup file and path
	bfile = backupPath + timestamp + backupName + '.gz'
	f=gzip.open(bfile,'w')
	# Temporary dictionary collapsed from d
	td = collapseDict(d)
	# Creates a list from the collapsed queue def dictionary and pickles it into the file
	pickle.dump([td[i] for i in td],f)
	f.close()
	if pickleDebug: print 'Ending pickle creation'
	return 0

def backupRestore(fname):
	''' Restore a DB backup pickle into the live database! Caution! If you err, you will hose the database! '''
	try:
		# Set up the file
		f=gzip.open(backupPath+fname+'.gz')
	except IOError:
		print 'File %s is not found at %s. Exiting.' % (fname+'.gz', backupPath)
		return 1
	try:
		# And load the pickle inside, if possible
		l=pickle.load(f)
		f.close()
	except:
		print 'File %s%s not in backup/pickle format. Exiting.' % (fname+'.gz', backupPath)
		return 1
	# Enable the DB
	utils.initDB()
	# Clear the DB
	#utils.dictcursor().execute('DELETE FROM schedconfig')
	print 'Updating SchedConfig'
	#Insert the queues as per the backup
	utils.replaceDB('schedconfig',l,key=dbkey)
	# Commit both the deletion and the changes
	utils.commit()
	utils.endDB()
	return 0
