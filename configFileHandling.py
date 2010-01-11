import os

from miscUtils import *
from controllerSettings import *

#----------------------------------------------------------------------#
# Config File Handling
#----------------------------------------------------------------------
def allMaker(d):
	'''Extracts commonalities from sites for the All files.
	Returns 0 for success. Adds "All" queues to sites. Updates the
	provenance info in the input dictionary. '''
	print 'Starting AllMaker'
	print d['TW']['Australia-ATLAS'].keys()
	all_d = {}
	# This is where we'll put all verified keys that are common across sites/clouds
	for cloud in [i for i in d.keys() if (i is not All and i is not ndef)]:
		# Create a dictionary for each cloud 
		all_d[cloud]={}
		# For all regular sites:
		for site in [i for i in d[cloud].keys() if (i is not All and i is not ndef)]:
			# Set up a site output dictionary
			all_d[cloud][site]={}
			# Recreate the site comparison queue
			comp = {}
			# Loop over all the queues in the site, where the queue is not empty or "All"
			for queue in [i for i in d[cloud][site].keys() if (i is not All and i is not ndef)]:
				# Create the key in the comparison dictionary for each parameter, if necessary, and assign a list that will hold the values from each queue 
				if not len(comp): comp = dict([(i,[d[cloud][site][queue][param][i]]) for i in d[cloud][site][queue][param].keys() if i not in excl])
				else: 
					# Fill the lists with the values for the keys from this queue
					for key in d[cloud][site][queue][param]:
						if key not in excl:
							try:
								comp[key].append(d[cloud][site][queue][param][key])
							except KeyError:
								comp[key] = [d[cloud][site][queue][param][key]]

			# Now, for the site, remove all duplicates in the lists. 
			for key in comp:
				# If only one value is left, it is common to all queues in the site
				if len(reducer(comp[key])) == 1:
					# So write it to the output for this cloud and site.
					all_d[cloud][site][key] = reducer(comp[key])[0]

	# Running across sites to update source information in the main dictionary
	for cloud in d.keys():
		for site in [i for i in d[cloud].keys() if (i is not All and i is not ndef)]:
			# No point in making an All file for one queue definition:
			if len(d[cloud][site]) > 1:
				# Extract all affected keys for the site
				skeys = all_d[cloud][site].keys()
				# Going queue by queue, update the provenance for both cloud and site general parameters.
				for queue in [i for i in d[cloud][site].keys() if (i is not All and i is not ndef)]:
					for key in skeys:
						d[cloud][site][queue][source][key] = 'the site All.py file for the %s site' % site
					try:
						if site == 'Australia-ATLAS': print 1, queue, d['TW']['Australia-ATLAS'][All][over], d[cloud][site].keys()
					except KeyError:
						pass
				# Adding the "All" queue to the site
				d[cloud][site][All] = {param:all_d[cloud][site]}
				try:
					if site == 'Australia-ATLAS': print 2, queue, d['TW']['Australia-ATLAS'][All][over], d[cloud][site].keys()
				except KeyError:
					pass
				if not d[cloud][site][All].has_key(over): d[cloud][site][All][over] = {}

				try:
					if site == 'Australia-ATLAS': print 3, queue, d['TW']['Australia-ATLAS'][All][over], d[cloud][site].keys()
				except KeyError:
					pass
			

	return 0

def composeFields(d,s,dname,allFlag=0):
	''' Populate a list for file writing that prints parameter dictionaries cleanly,
	allowing them to be written to human-modifiable config files for queues and sites.'''

	# "dname" is one of three things -- "Parameters" and "Override", depending on what part of the  
	# file we're writing. They're defined generally as param and over. A third, JDL, applies only to jdllist imports
	# and replaces param
	if dname == jdl: primary_key = 'name'
	else: primary_key = dbkey
	keylist = d[dname].keys()
	try:
		# Remove the DB key and put in as the first parameter -- this will be "nickname", usually.
		keylist.remove(primary_key)
		keylist.sort()
		keylist.insert(0,primary_key)

	# Unless it's not present -- then we'll just throw a warning.	 
	except ValueError:
		keylist.sort()
		# No point in warning for override or All dictionaries
		if dname == param and not allFlag:
			print 'DB key %s not present in this dictionary. Going to be hard to insert. %s' % (primary_key, str(d))

	# So we're writing a  "Parameters" or "Override" dictionary (dname)...
	s.append('%s = {' % dname + os.linesep )
	s_aside = []
	for key in keylist:
		if key not in excl:
			comment = ''
			value = str(d[dname][key])
			# Sanitize the inputs (having some trouble with quotes being the contents of a field):
			value = value.strip("'")
			if value == None: value = ''
			# For each key and value, check for multiline values, and add triple quotes when necessary 
			if value.count(os.linesep):
				valsep = "'''"
			else:
				valsep = keysep
			# If the value is being set somewhere other than the config files, comment it and send it to the bottom of the list
			if dname == param and d.has_key(source) and d[source][key] is not 'Config':
				# Add a comment to the line with the provenance info 
				comment = ' # Defined in %s' % d[source][key]
				s_aside.append(spacing + keysep + key + keysep + dsep + valsep + value + valsep + pairsep + comment + os.linesep)
			else: s.append(spacing + keysep + key + keysep + dsep + valsep + value + valsep + pairsep + comment + os.linesep)
	# Add in all the commented fields
	s.insert(0,'\n')
	s.extend(s_aside)
	# Complete the dictionary
	s.append(spacing + '}' + os.linesep)
	s.append(os.linesep)
	return 0

def makeConfigs(d):
	''' Reconstitutes the configuration files from the passed dictionary'''
	for cloud in d:
		# The working cloud path is made from the config base
		cloudpath = configs + os.sep + cloud
		# Create the config path for each site 
		for site in d[cloud]:
			# The working site path is made from the config base and cloud path
			path = cloudpath + os.sep + site
			# Try to make the path. If this fails, the path already exists.
			try:
				os.makedirs(path)
			except OSError:
				pass
			# Go on in...
			os.chdir(path)
			# And for each queue, create a config file. 
			for queue in d[cloud][site]:
				buildFile(queue, d[cloud][site][queue])
	return

def buildFile(name, d):
	'''Consolidate the composition and writing of the files'''
	startstr = '''
# This dictionary contains the parameters for one queue.
# Changing any parameter will update it in the schedconfig database.
# If you want to change a value temporarily, preserving the previous
# value, please put that parameter (key and new value) in the Override
# dictionary. Any value in Override will supersede any value in the
# Parameters dictionary.

# Parameters that have a comment appended ARE BEING SET ELSEWHERE!
# You can try to change them here, but it will FAIL!
# If you want to override one of these values, use the Override dictionary.

# Global values for entire sites can also be set
# (and overriden) in the "All" files within these directories.

'''
	overridestr = '''
# PLEASE USE FOR TEMPORARY CHANGES TO A QUEUE OR SITE
# This dictionary will override any queue value within its scope.
# If this override dictionary is part of an "All" file,
# key:value pairs included in it will override all other
# "Parameter" values. "Override" values in individual queues
# will supersede values specified here.

'''

	switchstr = 'Enabled = '
	# Load up the file intro
	s=[startstr]
	# Put the queue on/off switch in place if not an All file
	if name is not All: s.append(switchstr + d[enab] + '\n\n')
	if name == All: allFlag = 1
	else: allFlag = 0
	# I'm taking advantage of the universality of lists.
	# composeFields is modifying the list itself rather than a copy.
	# Since the params in the All files have no keys to find, we warn
	# the composeFields code.
	composeFields(d, s, param, allFlag)
	s.append(overridestr)
	composeFields(d, s, over)
	
	f=file(name + postfix,'w')
	f.writelines(s)
	f.close()
