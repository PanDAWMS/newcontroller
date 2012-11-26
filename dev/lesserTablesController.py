##########################################################################################
# Tools for controlling the content of the other tables in the atlas_pandameta database #
#                                                                                        #
# Alden Stradling 13 Jan 2011                                                             #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

import os

from dbAccess import utils
from configFileHandling import *
from controllerSettings import *
from svnHandling import *

def lesserTableAdder(d, tablename, primarykey):
	'''Returns the values in the schedconfig db as a dictionary'''
	utils.initDB()
	# Signal the different DB access
	print "Init DB %s"
	# Get all the fields
	query = "select * from %s" % tablename
	nrows = utils.dictcursor().execute(query)
	if nrows > 0:
		rows = utils.dictcursor().fetchall()
	utils.endDB()
	# Use the same dictionary form
	if lesserDebug: print 'Dictionary Created'
	# Populate this (much simpler) dictionary with the table's fields.
	# primarykey is the table's primary key (!!)
	for i in rows:
		if lesserDebug: print i[primarykey]
		d[i[primarykey]]={tablename:i,over:{}}
	# Sanitization
	for i in d:
		for key in d[i][tablename]:
			if type(d[i][tablename][key]) is str: d[i][tablename][key] = d[i][tablename][key].replace('\\n','\n')
	return 0

def buildLesserTableFiles(d, tablename, primarykey):
	'''Build the lesser table configuration files'''
	startstr = '''
# This dictionary contains the parameters for one spec.
# Changing this will update it in the %s table.
# If you want to change the value temporarily, preserving the previous
# value, please copy the new version into the Override
# dictionary. Override will supersede the %s dictionary.

''' % (tablename,tablename)
	overridestr = '''
# PLEASE USE FOR TEMPORARY CHANGES TO THE %s SPEC
# This dictionary will override any value within its scope.

''' % tablename
	# Put this in the right path -- back out from the code dir, and place it in pandaconf
	path = lesserconfigs + os.sep + tablename
	try:
		os.makedirs(path)
	except OSError:
		pass
	# Go to the directory
	os.chdir(path)
	# In contrast to the primary buildFile() (which does one at a time), this is a simpler
	# set of configs -- no clouds and sites to concern us. We'll do it all in one place, in
	# one fell swoop.
	for name in d:
		# Initiate the file string
		s=[startstr]
		# Use the same composeFields machinery as in the buildFiles -- build the main dict
		composeFields(d[name],s,tablename,primarykey)
		# Prep the override fields
		s.append(overridestr)
		# If any overrides have been detected, add them here.
		composeFields(d[name],s,over,primarykey)
		# Write each file in its turn as part of the for loop. The slashes are replaced with underscores
		# to keep the filesystem happy -- many of the lesser table names contain slashes. The double is to make
		# re-replacement easy.
		f=file(name.replace('/','__') + postfix,'w')
		f.writelines(s)
		f.close()

	
def buildLesserTableDict(tablename,primarykey):
	'''Build a copy of the lesser table dictionary from the configuration files '''

	path = lesserconfigs + os.sep + tablename
	lesser_db={}
	lesser_d={}
	# In executing files for variables, one has to put the variables in a contained, local context.
	locvars={}
	base = os.getcwd()
	# Loop throught the clouds in the base folder
	try:
		lessers = [i for i in os.listdir(path) if i.endswith(postfix) and not i.startswith('.')]
	except OSError:
		# If the configs folder is missing and this is the first thing run,
		# Reload this from the DB.
		# When SVN is in place, this should be replaced by a svn checkout.
		# We choose element 0 to get the first result. This hack will go away.
		print 'Fail! Please update lesser tables first!'
		return 1

		
	for j in lessers:
		# Add each lesser_ to the dictionary and remove the .py
		name = j.replace('__','/').rstrip(postfix)
		lesser_d[name] = {}
		# Run the file to extract the appropriate dictionaries
		# As a clarification, the lesser table and Override variable are created when the config python file is executed
		# The appropriate dictionaries are placed in locvars
		execfile(lesserconfigs+os.sep+tablename + os.sep + j,{},locvars)
		# Feed in the configuration
		lesser_d[name][tablename] = locvars[tablename]
		lesser_d[name][tablename][primarykey] = name
		lesser_d[name][over] = locvars[over] 
				
	os.chdir(base)
	return lesser_d

	
