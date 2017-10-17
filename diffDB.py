#! /usr/bin/env python
#######################################################
# Alden Stradling (Alden.Stradling@cern.ch) 18 Jul 10 #
#######################################################

from dbAccess import *
from dictHandling import *


def checkDB():
    '''Check the INTR DB against the Prod DB (testing before deployment)'''
    intr_db, standardkeys = sqlDictUnpacker(loadSchedConfig('intr', 1))
    intr_db = collapseDict(intr_db)
    prod_db, standardkeys = sqlDictUnpacker(loadSchedConfig('pmeta', 0))
    prod_db = collapseDict(prod_db)

    testDiff(intr_db, prod_db)

    return intr_db, prod_db


if __name__ == "__main__":
    dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())
    intr_db, prod_db = checkDB()
