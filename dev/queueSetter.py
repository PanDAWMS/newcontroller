#! /usr/bin/env python

###########################################
# queueSetter.py                          #
#                                         #
# Handy tool for setting queue parameters #
# in bulk.                                #
#                                         #
# Alden Stradling  30 Jul 2010            #
# Alden.Stradling@cern.ch                 #
##########################################

import os, sys

from controllerSettings import *
from miscUtils import *
from dictHandling import *
from configFileHandling import *
from svnHandling import *

def queueSetter(d, key, val, nick):
	''' When passed the key, value and nickname list, changes those nicknames\' parameter to that value.'''
	# The input dictiodnary needs to be a dictionary
	if not type(d) == dict:
		print 'You didn\'t pass in a dictionary. Try again.'
		sys.exit()
	
	# Testing the first dict entry's first entry to see if we have nested dicts.
	# The input dictionary needs to be collapsed. If it isn't, do it.
	if type(d[d.keys()[0]]) == dict: run_d=collapseDict(d)
	# Or we just use a copy of the dict.
	else run_d = d.copy()
	
	# Expand values list to match keys. Can be single value across multiple 
	if type(nick) not in (dict, list, tuple): nick=[nick]
	elif type(nick) is dict: sys.exit('Can\'t have it all come in as a dictionary')
	if type(key) not in (dict, list, tuple): key=[key]
	if type(key) is dict: key, val = key.keys(), key.values()
	if type(val) is not in (dict, list, tuple): val=[val for i in range(len(key))]
	elif type(val) is dict: sys.exit('Can\'t have values be a dictionary')
	if len(val) != len(nick): sys.exit('Values don\'t match parameters')

	
