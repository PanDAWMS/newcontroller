from SchedulerUtils import utils

from miscUtils import *
from controllerSettings import *

#----------------------------------------------------------------------#
# DB Access Methods 
#----------------------------------------------------------------------#
def loadSchedConfig():
	'''Returns the values in the schedconfig db as a dictionary'''
	utils.initDB()
	print "Init DB"
	query = "select * from schedconfig"
	nrows = utils.dictcursor().execute(query)
	if nrows > 0:
		rows = utils.dictcursor().fetchall()
	utils.endDB()
	d={}
	for i in rows:
		d[i[dbkey]]=i

	return d

# To be completed!!
def execUpdate(updateList):
	''' Run the updates into the schedconfig database '''
	if safety is "on":
		print "Not touching the database! The safety's on ol' Bessie."
		return 1
	utils.initDB()
	for query in updateList:
		utils.dictcursor().execute(query)
	utils.commit()
	utils.closeDB()
	return loadSchedConfig()
  
def buildUpdateList(updDict, tableName):
	'''Build a list of SQL commands to add or supersede queue definitions''' 
	
	matched = ' WHEN MATCHED THEN UPDATE SET '
	insert = ' WHEN NOT MATCHED THEN INSERT '
	values1 = ' VALUES '
	values2 = ' WITH VALUES '
	sql = []
	for key in updDict:
		merge = "MERGE INTO %s USING DUAL ON ( %s.nickname='%s' ) " % (tableName, tableName, key)
		mergetxt1 = ' %s ' % ','.join(['%s=:%s' % (i,i) for i in sorted(updDict[key].keys())])
		mergetxt2 = ' (%s) ' % ',:'.join(sorted(updDict[key].keys()))
		valuestxt = '{%s} ' % ', '.join(["'%s': '%s'" % (i,updDict[key][i]) for i in sorted(updDict[key].keys())])
		sql.append(merge+matched+mergetxt1+insert+values1+mergetxt2+values2+valuestxt+';')
		
	return '\n'.join(sql)

def buildDeleteList(delDict, tableName):
	'''Build a list of SQL commands that deletes queues no longer in the definition files'''
	#delstr='DELETE FROM atlas_pandameta.%s WHERE NICKNAME = '
	delstr='DELETE FROM %s WHERE NICKNAME = ' % tableName
	sql=[]
	for i in delDict:
		sql.append(delstr+delDict[i]['nickname']+';')
	return '\n'.join(sql)

