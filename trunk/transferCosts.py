##########################################################################################
# Tools for handling the transfer costs table in newController                           #
#                                                                                        #
# Alden Stradling 03 Oct 2012                                                            #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

import os, urllib
from SchedulerUtils import utils

from miscUtils import *
from controllerSettings import *
from dbAccess import *

#----------------------------------------------------------------------#
#----------------------------------------------------------------------#

# Format of the web input
headers = ['last_update','sourcesite','destsite','cost','status','type']

def loadTransferCostsWeb():
	'''Load the values extracted from Ilya Vukotic\'s web publication of site transfer costs'''
	transferCostURL = 'http://ivukotic.web.cern.ch/ivukotic/costMatrix/index.asp'
	# Defining the "type" that will be replaced
	web='http://ivukotic.web.cern.ch/ivukotic/WAN/index.asp'
	web_replacement = 'Web'

	# Fix the timestamp spacing
	transferCosts = [i.replace(' ','_',1).replace('_to_',' ').replace(web,web_replacement).split() for i in urllib.urlopen(transferCostURL)]
	# Create a dictionary that matches what will come from the DB
	return dict(((i[headers.index('sourcesite')],i[headers.index('destsite')],i[headers.index('type')]),
				 dict(zip(headers,i))) for i in transferCosts)
	

def loadTransferCostsDB():
	'''Load the values from the installedsw table into a dictionary keyed by release_site_cache'''
	#if safety is 'on': utils.setTestDB()
	utils.setTestDB()
	print 'Using INTR Database'
	utils.initDB()
	print "Init DB"
	# Gets all rows from transfercosts table
	query = 'SELECT * from transfercosts'
	nrows = utils.dictcursor().execute(query)
	if nrows > 0:
		# Fetch all the rows
		rows = utils.dictcursor().fetchall()
	# Close DB connection
	utils.endDB()
	# Return a dictionaried version of the DB contents, keyed release_site_cache_cmt
	unicodeConvert(rows)
	return dict([((i['sourcesite'],i['destsite'],i['type']),i) for i in rows])

def updateTransferCosts():
	'''Get the information from all sources and combine them to find updates vs. the DB'''
	# There may eventually be sources besides Web... and they can be added here when necessary.
	costs_db = loadTransferCostsDB()
	costs_web = loadTransferCostsWeb()
	deleteList = [costs_db[i] for i in costs_db if i not in costs_web]
	addList = [costs_web[i] for i in costs_web if i not in costs_db]
	return addList, deleteList

def updateTransferCostsDB(addList, deleteList):
	'''Populate any changes into the DB. Items that might conflict go into the History table'''
	#if safety is 'on': utils.setTestDB()
	pass


utils.setTestDB()
print 'Using INTR Database'
utils.initDB()
print "Init DB"
# Gets all rows from transfercosts table
query = 'SELECT * from transfercosts'
nrows = utils.dictcursor().execute(query)
if nrows > 0:
	# Fetch all the rows
	rows = utils.dictcursor().fetchall()
# Close DB connection
utils.endDB()
# Return a dictionaried version of the DB contents, keyed release_site_cache_cmt
unicodeConvert(rows)

c=dict([((i['sourcesite'],i['destsite'],i['type']),i) for i in rows])

addList, deleteList = updateTransferCosts()

utils.setTestDB()
print 'Using INTR Database'
utils.initDB()
print "Init DB"

for n,costDict in enumerate(addList):
	keys = ','.join(headers)
	values = [costDict[i] for i in headers]
	values[headers.index('last_update')] = "TO_TIMESTAMP('" + values[headers.index('last_update')] + "', 'YYYY-MM-DD_HH24:MI:SS')"
	# Add appropriate quotes. Remove the extraneous quotes for the timestamp.
	values = "'" + "','".join(values) + "'"
	sql1 = "INSERT INTO transfercosts (%s) VALUES (%s)" % (keys, values)
	sql = sql1.replace("'TO_TIMESTAMP","TO_TIMESTAMP").replace(")'",")")
	utils.dictcursor().execute(sql)
utils.commit()

# Close DB connection
utils.endDB()

	

a,b = updateTransferCosts()

## CREATE TABLE ATLAS_PANDAMETA.TRANSFERCOSTS
## (
## sourcesite VARCHAR2(256) constraint sourcesite_NN NOT NULL,
## destsite VARCHAR2(256) constraint destsite_NN NOT NULL,
## type VARCHAR2(256),
## status VARCHAR2(64),
## last_update DATE,
## cost NUMBER constraint cost_NN NOT NULL,
## max_cost NUMBER,
## min_cost NUMBER,
## constraint TRANSFERCOSTS_PK primary key(sourcesite, destsite, type) 
## ) 
## ORGANIZATION INDEX COMPRESS 1;


## CREATE TABLE ATLAS_PANDAMETA.TRANSFERCOSTS_HISTORY
## (
## sourcesite VARCHAR2(256) constraint sourcesite_hist_NN NOT NULL,
## destsite VARCHAR2(256) constraint destsite_hist_NN NOT NULL,
## type VARCHAR2(256),
## status VARCHAR2(64),
## last_update DATE,
## cost NUMBER constraint cost_hist_NN NOT NULL,
## max_cost NUMBER,
## min_cost NUMBER
## ) PCTFREE 0;

## CREATE INDEX ATLAS_PANDAMETA.TRANSFERCOSTS_HIST_INDX on ATLAS_PANDAMETA.TRANSFERCOSTS_HISTORY(sourcesite, destsite, type) COMPRESS 1;
## CREATE INDEX ATLAS_PANDAMETA.TRANSFERCOSTS_HIST_INDX2 on ATLAS_PANDAMETA.TRANSFERCOSTS_HISTORY(last_update);
