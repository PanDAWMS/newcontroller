##########################################################################################
# Tools for handling database operations in newController                                #
#                                                                                        #
# Alden Stradling 10 Oct 2009                                                            #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

from SchedulerUtils import utils

from miscUtils import *
from controllerSettings import *

#----------------------------------------------------------------------#
# DB Access Methods 
#----------------------------------------------------------------------#

def loadSchedConfig(db='intr', test='0'): 
	'''Returns the values in the schedconfig db as a dictionary'''
	# Initialize DB
	utils.dbname=db
	if safety is 'on': utils.setTestDB()
	utils.initDB()
	print "Init DB %s " % utils.dbname
	# Gets all rows from schedconfig table
	query = "select * from schedconfig"
	nrows = utils.dictcursor().execute(query)
	if nrows > 0:
		# Fetch all the rows
		rows = utils.dictcursor().fetchall()
	# Close DB connection
	utils.endDB()
	d={}
	for i in rows:
		# Lower-casing all of the DB keys for consistency upon read
		newd=dict([(key.lower(),i[key]) for key in i])
		# Populate the output dictionary with queue definitions, keyed by queue nickname
		d[newd[dbkey]]=newd

	unicodeConvert(d)
	return d

def loadInstalledSW():
	'''Load the values from the installedsw table into a dictionary keyed by release_site_cache'''
	if safety is 'on': utils.setTestDB()
	print "Init DB %s " % utils.dbname
	utils.dbname='intr'
	utils.initDB('intr')

	# Gets all rows from installedsw table
	query = 'SELECT * from installedsw'
	nrows = utils.dictcursor().execute(query)
	if nrows > 0:
		# Fetch all the rows
		rows = utils.dictcursor().fetchall()
	# Close DB connection
	utils.endDB()
	# Return a dictionaried version of the DB contents, keyed release_site_cache
	unicodeConvert(rows)
	return dict([('%s_%s_%s' % (i['siteid'],i['release'],i['cache']),i) for i in rows])

def updateInstalledSWdb(addList, delList):
	'''Update the installedsw table of pandameta by deleting obsolete releases and adding new ones'''
	unicodeEncode(addList)
	unicodeEncode(delList)

	delList=[i for i in delList]

	
	if safety is 'on': utils.setTestDB()
	utils.initDB('intr')
	print "Init DB %s " % utils.dbname
	for i in addList:
		sql="INSERT INTO installedsw (SITEID,CLOUD,RELEASE,CACHE) VALUES ('%s','%s','%s','%s')" % (i['siteid'],i['cloud'],i['release'],i['cache'])
		try:
			utils.dictcursor().execute(sql)
		except:
			print "SQL failed: %s" % sql 
		
	for i in delList:
		sql="DELETE FROM installedsw WHERE siteid = '%s' and release = '%s' and cache = '%s'" % (i['siteid'],i['release'],i['cache'])
		if i['cache'] is None: sql="DELETE FROM installedsw WHERE siteid = '%s' and release = '%s' and cache is NULL" % (i['siteid'],i['release'])
		utils.dictcursor().execute(sql)
		
	utils.commit
	utils.endDB()

def execUpdate(updateList):
	''' Run the updates into the schedconfig database -- does not use bind variables. Use replaceDB for large replace ops.'''
	if safety is 'on': utils.setTestDB()
	utils.initDB('intr')
	print "Init DB %s " % utils.dbname
	for query in updateList:
		# Each update is pre-rolled -- just gets executed
		utils.dictcursor().execute(query)
	# Commit all the updates
	utils.commit()
	utils.closeDB()
	return 

def buildUpdateList(updDict,param):
	'''Build a list of dictionaries that define queues''' 
	l=[]
	for i in updDict:
		# Gets only the parameter dictionary part.
		if param in updDict[i]:
			l.append(updDict[i][param])
			l[-1][dbkey] = i
		else: l.append(updDict[i])
		# Fix any NULL values being sent to the DB. The last row added on each loop is checked.
	for i in l:
		for key in i:
			if i[key] == None and key in nonNull.keys():
				i[key] = nonNull[key]
				
	return l
	

def buildDeleteList(delDict, tableName, key = dbkey):
	'''Build a list of SQL commands that deletes queues no longer in the definition files. Key defaults to dbkey'''
	delstr='DELETE FROM %s WHERE %s = ' % (tableName,dbkey)
	sql=[]
	for i in delDict:
	# Build delete queries from an existing dict. Deletes by DB key (or other specification, if ever necesssary).
		sql.append("%s'%s'" % (delstr,delDict[i][key]))
	return sql
