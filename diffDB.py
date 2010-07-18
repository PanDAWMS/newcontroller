#! /usr/bin/env python
#######################################################
# Alden Stradling (Alden.Stradling@cern.ch) 23 Jun 09 #
#######################################################

import os, sys, commands, pickle

from controllerSettings import *
from miscUtils import *
from dbAccess import *
from dictHandling import *

def checkDB():
	'''Check the INTR DB against the Prod DB (testing before deployment)'''
	prod_db, standardkeys = sqlDictUnpacker(loadSchedConfig())
	prod_db = collapseDict(prod_db)
	intr_db, standardkeys = sqlDictUnpacker(loadSchedConfig('pmeta',0))
	intr_db = collapseDict(intr_db)

	testDiff(intr_db, prod_db)
	
	return intr_db, prod_db
	

if __name__ == "__main__":

	dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())
	intr_dn, prod_db = checkDB()
