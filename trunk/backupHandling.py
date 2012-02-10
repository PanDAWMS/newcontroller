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
	# Check for all the CSV files created recently, and select the most recent lastVolatiles.
	l = [i for i in sorted(os.listdir(hotBackupPath)) if volatileCSVName in i][:lastVolatiles]
	files = dict([i,gzip.open(hotBackupPath+i) for i in l])
	readers = dict([i,files[i] for i in l])
	master = {}
	for r in readers:
		headers = readers[r].next()
		for queue in readers[r]:
			if not master.has_key(queue[0]): master[queue[0]] = {r.split(volatileCSVName)[0].replace('_',''):dict(zip(headers,queue))}
			else: master[queue[0]][r.split(volatileCSVName)[0].replace('_','')] = dict(zip(headers,queue))
	
	
def volatileBackupCreate():
	'''Back up the volatile parts of the DB before every operation'''
	# Get the present status of schedconfig
	d=loadSchedConfig()
	try:
		os.stat(hotBackupPath)
	except:
		# And make one if not present
		os.makedirs(hotBackupPath)
	# Grab the backup's timestamp
	timestamp = '_'.join([str(i).zfill(2) for i in time.gmtime()[:-3]])+'_'
	# Create the backup file as a gzip file for space reasons
	bfile = hotBackupPath + timestamp + volatileSQLName + '.gz'
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
	bfile = hotBackupPath + timestamp + volatileCSVName + '.gz'
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
	''' Create  a backup insert and csv.gz file of queue restoration information''' 
	#''' Create a backup pickle file of a list of queue spec dictionaries that can be fed to direct DB updates''' 

	# Major refiguring from the original purpose. If there is something that needs to be restored, it's nice
	# to be able to do it by site, or cloud, or queue. Even doing it all when things are difficult. So the
	# backup will be stored as a gzip of sql statements, and as a CSV.gz of values. Both have virtures, and will
	# be small enough to keep a certain number of hot copies in AFS and 
	# Get the present status of schedconfig
	# Initialize DB
	
	
	d=loadSchedConfig()
	try:
		os.stat(hotBackupPath)
	except:
		# And make one if not present
		os.makedirs(hotBackupPath)
	# Grab the backup's timestamp
	timestamp = '_'.join([str(i).zfill(2) for i in time.gmtime()[:-3]])+'_'
	# Create the backup file as a gzip file for space reasons
	bfile = hotBackupPath + timestamp + backupSQLName + '.gz'
	f=gzip.open(bfile,'w')
	# The interesting fields do not contain a timestamp, and are not the nickname. :)
	fields = [i for i in d[d.keys[0]].keys() if i not in timestamps + [dbkey]]
	# For each of the queues, create a SQL restore command. The atlas_pandameta is unnecessary for restoration using the volatileRestore.py script, ans is stripped
	# off there... but this format allows direct copy-paste into SQL Developer or another direct SQL tool as well. This can be handy for manual or custom restores.
	# The Site and Cloud comments help filter the lines to allow manual restoration of specific sites and clouds using grep, avoiding a broad-brush approach.
	for i in d:
		f.write('UPDATE atlas_pandameta.schedconfig set %s WHERE nickname = \'%s\' -- Site is %s, Cloud is %s;\n' % (', '.join(['%s = \'%s\'' % (key, d[i][key]) for key in fields]), i, d[i]['site'],d[i]['cloud']))
	f.close()

	# Here we create a CSV file that allows a history of the fields to be kept. It goes into the scratch storage.
	bfile = longBackupPath + timestamp + backupCSVName + '.gz'
	# Open the file via gzip
	f = gzip.open(bfile,'w')
	# Attach a CSV writer to the file
	w = csv.writer(f)
	# Write the header row
	fields.insert(0,dbkey)
	w.writerow(fields)
	# Write all relevant queue data
	for i in d:
		w.writerow([d[i][key] for key in  fields])
	f.close()
	return 0
	
def backupRestore(fname):
	''' Restore a DB backup pickle into the live database! Caution! If you err, you will hose the database! '''
	try:
		# Set up the file
		f=gzip.open(hotBackupPath+fname+'.gz')
	except IOError:
		print 'File %s is not found at %s. Exiting.' % (fname+'.gz', hotBackupPath)
		return 1
	try:
		# And load the pickle inside, if possible
		l=pickle.load(f)
		f.close()
	except:
		print 'File %s%s not in backup/pickle format. Exiting.' % (fname+'.gz', hotBackupPath)
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
