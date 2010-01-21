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

def execUpdate(updateList):
	''' Run the updates into the schedconfig database -- does not use bind variables. Use replaceDB for large replace ops.'''
	if safety is "on":
		print "Not touching the database! The safety's on ol' Bessie."
		return 1
	utils.initDB()
	for query in updateList:
		utils.dictcursor().execute(query)
	utils.commit()
	utils.closeDB()
	return 

def buildUpdateList(updDict):
	'''Build a list of dictionaries that define queues''' 
	l=[]
	for i in updDict:
		l.append(updDict[i])
		
	return l
	

def buildDeleteList(delDict, tableName):
	'''Build a list of SQL commands that deletes queues no longer in the definition files'''
	#delstr='DELETE FROM atlas_pandameta.%s WHERE NICKNAME = '
	delstr='DELETE FROM %s WHERE NICKNAME = ' % tableName
	sql=[]
	for i in delDict:
		sql.append(delstr+delDict[i]['nickname']+';')
	return '\n'.join(sql)

