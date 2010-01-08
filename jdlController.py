import os

from dbAccess import util
from configFileHandling import *
from controllerSettings import *

def buildJdlFiles(d):
	'''Build th JDL configuration files'''
	startstr = '''
# This dictionary contains the parameters for one jdl spec.
# Changing this will update it in the jdllist table.
# If you want to change the value temporarily, preserving the previous
# value, please copy the new version into the Override
# dictionary. Override will supersede the Parameters dictionary.

'''
	overridestr = '''
# PLEASE USE FOR TEMPORARY CHANGES TO A JDL SPEC
# This dictionary will override any value within its scope.

'''
	# Put this in the right path -- back out from the code dir, and place it in pandaconf
	path = jdlconfigs

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
		composeFields(d[name],s,jdl)
		# Prep the override fields
		s.append(overridestr)
		# If any overrides have been detected, add them here.
		composeFields(d[name],s,over)

		# Write each file in its turn as part of the for loop. The slashes are replaced with underscores
		# to keep the filesystem happy -- many of the JDL names contain slashes. The double is to make
		# re-replacement easy.
		f=file(name.replace('/','__') + postfix,'w')
		f.writelines(s)
		f.close()
	
def buildJdlDict():
	'''Build a copy of the jdl dictionary from the configuration files '''

	jdld={}
	# In executing files for variables, one has to put the variables in a contained, local context.
	locvars={}
	base = os.getcwd()
	# Loop throught the clouds in the base folder
	try:
		jdls = [i for i in os.listdir(jdlconfigs) if i.endswith(postfix)]
	except OSError:
		# If the configs folder is missing and this is the first thing run,
		# Reload this from the DB.
		# When SVN is in place, this should be replaced by a svn checkout.
		# We choose element 0 to get the first result. This hack will go away.
		jdlListAdder()
		jdls = os.listdir(configs)

	for j in jdls:
		# Add each jdl to the dictionary and remove the .py
		name = j.replace('__','/').rstrip(postfix)
		jdld[name] = {}
		# Run the file to extract the appropriate dictionaries
		# As a clarification, the JDL and Override variable are created when the config python file is executed
		# The appropriate dictionaries are placed in locvars
		execfile(j,{},locvars)
		# Feed in the configuration
		jdld[name][jdl] = locvars[jdl]
		jdld[name][over] = locvars[over] 
				
	os.chdir(base)
	return jdld

def jdlListAdder(d):
	'''Returns the values in the schedconfig db as a dictionary'''
	utils.initDB()
	# Signal the different DB access
	print "Init DB (JDLLIST)"
	# Get all the fields
	query = "select * from jdllist"
	nrows = utils.dictcursor().execute(query)
	if nrows > 0:
		rows = utils.dictcursor().fetchall()
	utils.endDB()
	# Use the same dictionary form
	if jdlDebug: print 'Dictionary Created'
	# Populate this (much simpler) dictionary with the JDL fields.
	for i in rows:
		if jdlDebug: print i['name']
		d[i['name']]={jdl:i,over:{}}

	# There's no way to organize even by queue. The JDL will link to the
	# schedconfig queues by matching the jdl field to the name field
	# Sanitization
	for i in d:
		d[i][jdl]['jdl'] = d[i][jdl]['jdl'].replace('\\n','\n')
	return 0
