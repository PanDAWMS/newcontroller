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
	d=loadSchedConfig()
	try:
		os.stat(backupPath)
	except:
		# And make one if not present
		os.makedirs(backupPath)
	timestamp = '_'.join([str(i).zfill(2) for i in time.gmtime()[:-3]])+'_'
	bfile = backupPath + timestamp + volatileSQLName + '.gz'
	f=gzip.open(bfile,'w')
	for i in d:
		f.write('UPDATE atlas_pandameta.schedconfig set status = \'%s\', nqueue = %s, multicloud = \'%s\', sysconfig = \'%s\', dn = \'%s\', space = %s, comment_ = \'%s\' WHERE nickname = \'%s\';\n' % (d[i]['status'], d[i]['nqueue'],d[i]['multicloud'],d[i]['sysconfig'],d[i]['dn'],d[i]['space'],d[i]['comment_'], i))
	f.close()
	bfile = backupPath + timestamp + volatileCSVName + '.gz'
	f = gzip.open(bfile,'w')
	w = csv.writer(f)
	w.writerow(['nickname','status','nqueue','multicloud','sysconfig','dn','space','comment_'])
	for i in d:
		w.writerow([d[i]['status'], d[i]['nqueue'],d[i]['multicloud'],d[i]['sysconfig'],d[i]['dn'],d[i]['space'],d[i]['comment_']])
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
