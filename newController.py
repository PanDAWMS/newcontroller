#! /usr/bin/env python
#######################################################
# Handles storage and modification of queue data      #
# in the ATLAS PanDA schedconfig and jdllist tables   #
# for ATLAS production and analysis queues.           #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 23 Jun 09 #
#######################################################

# TODO:

# Add installedSW table support
# Add a consistency checker
# Add manual pickle restoration code
# Add subversion update, file add and checkin. Comment changes.
# Add change detection to avoid DB change collisions
# Add queue insertion scripts
# Add checking of queue "on" and "off"
# Make sure that the All source is subordinate to the BDII source
# Make sure that the jdladd field is fully commented when it's being removed from the source

# This code has been organized for easy transition into a class structure.

import pickle, os, sys, commands, re

from SchedulerUtils import utils

try:
	import dq2.info.TiersOfATLAS as ToA
except:
	print "Cannot import dq2.info.TiersOfATLAS, will exit"
	sys.exit(-1)

try:
	import lcgInfositeTool
except:
	print "Cannot import lcgInfositeTool, will exit"
	sys.exit(-1)

try:
	import fillDDMpaths
except:
	print "Cannot import fillDDMpaths, will exit"
	sys.exit(-1)


toaDebug = False
jdlDebug = False
bdiiDebug = False
dbReadDebug = False
dbWriteDebug = False
configReadDebug = False
configWriteDebug = False

safety = 'on'
All = 'All'
ndef = 'Deactivated'
param = 'Parameters'
over = 'Override'
jdl = 'JDL'
source = 'Source'
enab = 'Enabled'
base_path = os.getcwd()
# Step back a layer in the path for the configs, and put them in the config SVN directory
cfg_path = base_path[:base_path.rfind(os.sep)] + os.sep + 'pandaconf' + os.sep
backupPath = cfg_path + 'Backup'
backupName = 'schedConfigBackup.pickle'
configs = cfg_path + os.sep + 'SchedConfigs'
jdlconfigs = cfg_path + os.sep + 'JDLConfigs'
postfix = '.py'
dbkey, dsep, keysep, pairsep, spacing = 'nickname', ' : ', "'", ',', '    '  # Standard python spacing of 4
shared, unshared = 'shared','unshared'
excl = ['status','lastmod','dn','tspace','_comment']
standardkeys = []

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


# To be completed!! Needs to warn on lcgLoad missing
def loadBDII():
	'''Loads LCG site definitions from the BDII, and dumps them in a file called lcgQueueUpdate.py in the local directory.
	This file is executed (even if generating it failed this time) and populated a dictionary of queue definitions, which is
	returned.'''
	osgsites={}
	if os.path.exists('lcgLoad.py'):
		print 'Updating LCG sites from BDII'
		try:
			commands.getoutput('./lcgLoad.py > lcgload.log')
		except Exception, e:
			print 'Running lcgLoad.py failed:', e
			print 'Reusing existing lcgQueueUpdate.py'
		execfile('lcgQueueUpdate.py')
		print 'LCG Initial Load Complete'
	else:
		loadlcg = 0
	return osgsites
	  
# To be completed!!
def loadToA(queuedefs):
	'''Acquires queue config information from ToA and updates the values we have. Should be run last. Overrides EVERYTHING else.'''
	fillDDMpaths.fillDDMpaths(queuedefs)
	return 0

def unPickler(fname):
	os.chdir(base_path)
	f=file(fname)
	t=pickle.load(f)
	f.close()
	d={}
	for i in t:
		d[i[dbkey]]=i
	return d
	
def pickleBackup(d):
	'''Pickle the schedconfigdb as a backup'''
	try:
		os.makedirs(backupPath)
	except OSError:
		pass
	os.chdir(backupPath)
	f=file(backupName, 'w')
	pickle.dump(d,f)
	f.close()

def protoDict(queue,d,sourcestr='DB',keys=[]):
	'''Create a dictionary with params, overrides and sources for either an existing definition from the DB, or to add the dictionaries
	for a new queue. Used in sqlDictUnpacker for extraction of DB values (default) and in bdiiIntegrator for new queue addition from the BDII.'''
	en_val = 'True'
	if not len(d):
		en_val = 'False'
		d = {queue:dict([(key,None) for key in keys])}
	return {param:d[queue],over:{},source:dict([(key,sourcestr) for key in d[queue].keys() if key not in excl]),enab:en_val}
	
def sqlDictUnpacker(d):
	'''Unpack the dictionary returned by Oracle or MySQL''' 
	# Organize by cloud, then site, then queue. Reading necessary data by key.
	# Remember that these vars are limited in scope.
	cloud = 'cloud'
	site = 'site'
	out_d={}
	stdkeys = []
	# Run over the DB queues
	for queue in d:
		# If the present queue's cloud isn't in the out_d, create the cloud.
		if d[queue][cloud] not in out_d:
			out_d[d[queue][cloud]] = {}
		# If the present queue's site isn't in the out_d cloud, create the site in the cloud.
		if d[queue][site] not in  out_d[d[queue][cloud]]:
			out_d[d[queue][cloud]][d[queue][site]] = {}

		# Once sure that we have all the cloud and site dictionaries created, we populate them with a parameter dictionary
		# an empty (for now) override dictionary, and a source dict. The override dictionary will become useful when we are reading back from
		# the config files we are creating. Each key is associated with a source tag -- Config, DB, BDII, ToA, Override, Site, or Cloud
		# That list comprehension at the end of the previous line just creates an empty dictionary and fills it with the keys from the queue
		# definition. The values are set to DB, and will be changed if another source modifies the value.
		out_d[d[queue][cloud]][d[queue][site]][d[queue][dbkey]] = protoDict(queue,d)
	
		# Model keyset for creation of queues from scratch
		# Append these new keys to standardkeys
		stdkeys.extend([key for key in d[queue].keys() if key not in excl])
	# Then remove all duplicates
	stdkeys=reducer(stdkeys)
	# Parse the dictionary to create an All queue for each site
	status = allMaker(out_d)
	# Take care of empty clouds (which are used to disable queues in schedconfig, for now) 
	# allMaker has to run before this to avoid causing KeyErrors with the new "empty cloud" values 
	if out_d.has_key(''):
		out_d[ndef]=out_d.pop('')
	if out_d.has_key(None):
		out_d[ndef]=out_d.pop(None)

	return out_d, stdkeys

## def jdlDictUnpacker(d):
## 	'''Unpack the dictionary returned by Oracle or MySQL for the jdllist table''' 
## 	# Reading necessary data by key.
## 	# Remember that these vars are limited in scope.
## 	out_d={}
## 	# Run over the jdl entries
## 	for entry in d:
## 		# Create the entry
## 		out_d[d[entry]['name']] = d[entry].copy()

## 	return out_d

def reducer(l):
	''' Reduce the entries in a list by removing dupes'''
	return dict([(i,1) for i in l]).keys()


def findQueue(q,d):
	'''Find a queue in the config dictionary and return the cloud and site. If not found, return empty values'''
	for cloud in d:
		for site in d[cloud]:
			# Not going to find sites in the All file
			if site == All:
				continue
			for queue in d[cloud][site]:
				if queue == q:
					return cloud, site
	return '',''

# Rewrite this to be more efficient -- it needs to parse the ddmsites once into a dictionary, then do matchmaking.
def toaIntegrator(confd):
	''' Adds ToA information to the confd (legacy from Rod, incomplete commenting. Will enhance later.) '''
	print 'Running ToA Integrator'
	for cloud in confd:
		for site in confd[cloud]:
			if site is All: continue
			for queue in confd[cloud][site]:
				if queue is All: continue
				try:
					if toaDebug and confd[cloud][site][queue][param]['sysconfig'] == 'manual': print 'Skipping %s as a manual queue (%s, %s)' % (queue, cloud, site) 
					# Make ToA check the 'manual' sysconfig flag
					if confd[cloud][site][queue][param]['sysconfig'] == 'manual': continue
					if ToA and (not confd[cloud][site][queue][param].has_key('ddm') or (not utils.isFilled(confd[cloud][site][queue][param]['ddm']))):
						ddmsites = ToA.getAllDestinationSites()
						for ds in ddmsites:
							gocnames = ToA.getSiteProperty(ds,'alternateName')
							if not gocnames: gocnames=[]
							 # upper case for matching
							gocnames_up=[]
							for gn in gocnames:
								gocnames_up+=[gn.upper()]  
							# If PRODDISK found use that
							if confd[cloud][site][queue][param]['site'].upper() in gocnames_up and ds.endswith('PRODDISK'):
								if toaDebug: print "Assign site %s to DDM %s" % ( confd[cloud][site][queue][param]['site'], ds )
								confd[cloud][site][queue][param]['ddm'] = ds
								confd[cloud][site][queue][source]['ddm'] = 'ToA'
								confd[cloud][site][queue][param]['setokens'] = 'ATLASPRODDISK'
								confd[cloud][site][queue][source]['setokens'] = 'ToA'
								# Set the lfc host 
								re_lfc = re.compile('^lfc://([\w\d\-\.]+):([\w\-/]+$)')
								if toaDebug: print "ds:",ds
								try:
									relfc=re_lfc.search(ToA.getLocalCatalog(ds))
									if relfc:
										lfchost=relfc.group(1)
										if toaDebug: print "Set lfchost to %s"%lfchost 
										confd[cloud][site][queue][param]['lfchost'] = lfchost
										confd[cloud][site][queue][source]['lfchost'] = 'ToA'
									else:
										if toaDebug: print " Cannot get lfc host for %s"%ds
								except:
									if toaDebug: print " Cannot get local catalog for %s"%ds
								# And work out the cloud
								if not confd[cloud][site][queue][param].has_key('cloud'): confd[cloud][site][queue][param]['cloud'] = ''
								if not confd[cloud][site][queue][param]['cloud']:
									if toaDebug: print "Cloud not set for %s"%ds
									for cl in ToA.ToACache.dbcloud.keys():
										if ds in ToA.getSitesInCloud(cl):
											confd[cloud][site][queue][param]['cloud']=cl
											confd[cloud][site][queue][source]['cloud'] = 'ToA'

					# EGEE defaults
					if confd[cloud][site][queue][param]['sysconfig'] == 'manual': print 'How did we get here?'
					if confd[cloud][site][queue][param]['region'] != 'US':
						# Use the pilot submitter proxy, not imported one (Nurcan non-prod) 
						confd[cloud][site][queue][param]['proxy']  = 'noimport'
						confd[cloud][site][queue][source]['proxy'] = 'ToA'
						confd[cloud][site][queue][param]['lfcpath'] = '/grid/atlas/users/pathena'
						confd[cloud][site][queue][source]['lfcpath'] = 'ToA'
						confd[cloud][site][queue][param]['lfcprodpath'] = '/grid/atlas/dq2'
						confd[cloud][site][queue][source]['lfcprodpath'] = 'ToA'
						if not confd[cloud][site][queue][param].has_key('copytool'):
							confd[cloud][site][queue][param]['copytool'] = 'lcgcp'
							confd[cloud][site][queue][source]['copytool'] = 'ToA'
						if confd[cloud][site][queue][param].has_key('ddm') and confd[cloud][site][queue][param]['ddm']:
							ddm1 = confd[cloud][site][queue][param]['ddm'].split(',')[0]
							if toaDebug: print 'ddm: using %s from %s'%(ddm1,confd[cloud][site][queue][param]['ddm'])
							# Set the lfc host 
							re_lfc = re.compile('^lfc://([\w\d\-\.]+):([\w\-/]+$)')

							if ToA:
								loccat = ToA.getLocalCatalog(ddm1)
								if loccat:
									relfc=re_lfc.search(loccat)
									if relfc :
										lfchost=relfc.group(1)
										if toaDebug: print "ROD sets lfchost for %s %s"%(confd[cloud][site][queue][param]['ddm'],lfchost) 
										confd[cloud][site][queue][param]['lfchost'] = lfchost
										confd[cloud][site][queue][source]['lfchost'] = 'ToA'
									else:
										if toaDebug: print " Cannot get lfc host for %s"%ddm1

								srm_ep = ToA.getSiteProperty(ddm1,'srm')
								if toaDebug: print 'srm_ep: ',srm_ep
								if not srm_ep:
									continue

								re_srm_ep=re.compile('(srm://[\w.\-]+)(/[\w.\-/]+)?/$')
								# Allow srmv2 form 'token:ATLASDATADISK:srm://srm.triumf.ca:8443/srm/managerv2?SFN=/atlas/atlasdatadisk/'
								re_srm2_ep=re.compile('(token:\w+:(srm://[\w.\-]+):\d+/srm/managerv\d\?SFN=)(/[\w.\-/]+)/?$')

								resrm_ep=re_srm_ep.search(srm_ep)
								resrm2_ep=re_srm2_ep.search(srm_ep)
								if resrm_ep:
									if toaDebug: print "ROD: srmv1"
									se=resrm_ep.group(1)
									sepath=resrm_ep.group(2)
									copyprefix=se+'/^dummy'
								elif resrm2_ep:
									se=resrm2_ep.group(1)
									copyprefix=resrm2_ep.group(2)+'/^dummy'
									sepath=resrm2_ep.group(3)


								if resrm_ep or resrm2_ep:
									if toaDebug: print  'SRM: ',se,sepath,copyprefix               
									if not confd[cloud][site][queue][param].has_key('se'):
										confd[cloud][site][queue][param]['se'] = se
										confd[cloud][site][queue][source]['se'] = 'ToA'
									if not confd[cloud][site][queue][param].has_key('sepath'):
										confd[cloud][site][queue][param]['sepath'] = sepath+'/users/pathena'
										confd[cloud][site][queue][source]['sepath'] = 'ToA'
									if not confd[cloud][site][queue][param].has_key('seprodpath'):
										confd[cloud][site][queue][param]['seprodpath'] = sepath
										confd[cloud][site][queue][source]['seprodpath'] = 'ToA'
									if not confd[cloud][site][queue][param].has_key('copyprefix'):
										confd[cloud][site][queue][param]['copyprefix'] =  copyprefix
										confd[cloud][site][queue][source]['copyprefix'] = 'ToA'
							else:
								if toaDebug: print 'DDM not set for %s'% confd[cloud][site][queue][param]['nickname']

							if not confd[cloud][site][queue][param].has_key('copysetup') or confd[cloud][site][queue][param]['copysetup']=='':
								confd[cloud][site][queue][param]['copysetup'] = '$VO_ATLAS_SW_DIR/local/setup.sh'
								confd[cloud][site][queue][source]['copysetup'] = 'ToA'
				except:
					if site is All or queue is All:
						print "Missing something"
						print cloud, site, queue
						print confd[cloud][site][queue]
					else:
						if toaDebug: print 'Failed to try'
						if toaDebug: print cloud, site, queue, confd[cloud][site][queue][param]
						
	print 'Finished ToA integrator'
	return

def bdiiIntegrator(confd,d):
	'''Adds BDII values to the configurations, overriding what was there. Must be run after downloading the DB
	and parsing the config files.'''
	print 'Running BDII Integrator'
	out = {}
	# Load the queue names, status, gatekeeper, gstat, region, jobmanager, site, system, jdladd 
	bdict = loadBDII()
	# Moving on from the lcgLoad sourcing, we extract the RAM, nodes and releases available on the sites 
	if bdiiDebug: print 'Running the LGC SiteInfo tool'
	linfotool = lcgInfositeTool.lcgInfositeTool()
	if bdiiDebug: print 'Completed the LGC SiteInfo tool run'

	# Defining a release dictionary
	rellist={}
	
	# Load the site information directly from BDII and hold it. In the previous software, this was the osgsites dict.
	# This designation is obsolete -- this is strictly BDII information, and no separation is made.
	for qn in bdict:
		# Create the nickname of the queue using the queue designation from the dict, plus the jobmanager.
		nickname = '-'.join([qn,bdict[qn]['jobmanager']]).rstrip('-')
		# Try to find the queue in the configs dictionary
		c,s = findQueue(nickname,confd)
		if not c and not s:
			# If it's not there, try the dictionary from the DB dictionary
			if bdiiDebug: print "Couldn't find ", nickname
			c,s = findQueue(nickname,d)
			# If the queue is not in the DB, and is not inactive in the config files, then:
			if not c and not s:
				if bdiiDebug: print "Still couldn't find ", nickname
				# If a site is specified, go ahead
				if bdict[qn]['site']: c,s = ndef,bdict[qn]['site']
				# If not, time to give up. BDII is hopelessly misconfigured -- best not to contaminate
				else: continue
				# If site doesn't yet exist, create it:
				if s not in confd[c]:
					if bdiiDebug: print 'Creating site %s in cloud %s as requested by BDII'  % (s,c)
					confd[c][s] = {}
					# Create it in the main config dictionary, using the standard keys from the DB (set in the initial import)
				print 'Creating queue %s in site %s and cloud %s as requested by BDII. This queue must be enabled by hand.' % (nickname, s, c)
				confd[c][s][nickname] = protoDict(nickname,{},sourcestr='Queue created by BDII',keys=standardkeys)
				confd[c][s][nickname][enab] = 'False'
				# Either way, we need to put the queue in without a cloud defined. 
		# Check for manual setting. If it's manual, DON'T TOUCH
		if confd[c][s][nickname][param]['sysconfig']:
			if confd[c][s][nickname][param]['sysconfig'].lower() == 'manual':
				if bdiiDebug: print 'Skipping %s -- sysconfig set to manual' % nickname
				continue
		if confd[c][s][nickname][param]['sysconfig'] == 'manual':
			print 'Real problem! This manual queue is being edited'
		# For all the simple translations, copy them in directly.
		for key in ['localqueue','system','status','gatekeeper','jobmanager','jdladd','site','region','gstat']:
			confd[c][s][nickname][param][key] = bdict[qn][key]
			# Complete the sourcing info
			confd[c][s][nickname][source][key] = 'BDII'
		# For the more complicated BDII derivatives, do some more complex work
		confd[c][s][nickname][param]['queue'] = bdict[qn]['gatekeeper'] + '/jobmanager-' + bdict[qn]['jobmanager']
		if bdict[qn]['gatekeeper'] + '/jobmanager-' + bdict[qn]['jobmanager'] != confd[c][s][nickname][param]['jdl']:
			print 'jdl mismatch!'
			print bdict[qn], key, confd[c][s][nickname][param]['jdl'], 
		confd[c][s][nickname][param]['jdl'] = bdict[qn]['gatekeeper'] + '/jobmanager-' + bdict[qn]['jobmanager']
		confd[c][s][nickname][param]['nickname'] = nickname
		# Fill in sourcing here as well for the last few fields
		for key in ['queue','jdl','nickname']:
			confd[c][s][nickname][source][key] = 'BDII'
		
		tags=linfotool.getSWtags(confd[c][s][nickname][param]['gatekeeper'])
		etags=linfotool.getSWctags(confd[c][s][nickname][param]['gatekeeper'])
		if len(etags) > 0:
			for erelease in etags:
				try:
					cache=erelease.replace('production','AtlasProduction').replace('tier0','AtlasTier0') 
					release='.'.join(erelease.split('-')[1].split('.')[:-1])
					idx = '%s_%s' % (confd[c][s][nickname][param]['site'],cache)
					rellist[idx]={'site':confd[c][s][nickname][param]['site']}
					rellist[idx]['release'] = release
					rellist[idx]['cache'] = cache
					rellist[idx]['siteid'] = '' # to fill later, when this is available
					rellist[idx]['nickname'] = confd[c][s][nickname][param]['nickname'] # To reference later, when we need siteid
					rellist[idx]['gatekeeper'] = confd[c][s][nickname][param]['gatekeeper']
				except KeyError:
					if bdiiDebug: print confd[c][s][nickname][param]
		else:
			if bdiiDebug: print("No eTags!")

		if len(tags) > 0:
			for release in tags:
				try:
					idx = '%s_%s' % (confd[c][s][nickname][param]['site'],release)
					rellist[idx]={'site':confd[c][s][nickname][param]['site']}
					rellist[idx]['release'] = release
					rellist[idx]['cache'] = ''
					rellist[idx]['siteid'] = '' # to fill later, when this is available
					rellist[idx]['nickname'] = confd[c][s][nickname][param]['nickname'] # To reference later, when we need siteid
					rellist[idx]['gatekeeper'] = confd[c][s][nickname][param]['gatekeeper']
				except KeyError:
					if bdiiDebug: print release, idx, confd[c][s][nickname][param]
		else:
			if bdiiDebug: print("No Tags for %s!" % nickname)
			pass
			
		if len(tags) > 0:
			releases = '|'.join(tags)
			confd[c][s][nickname][param]['releases']=releases
			confd[c][s][nickname][source]['releases'] = 'BDII'
		else:
			if bdiiDebug: print "No releases found for %s"% confd[c][s][nickname][param]['gatekeeper']


	# All changes to the dictionary happened live -- no need to return it.
	print 'Finished BDII Integrator'
	return 0

def allMaker(d):
	'''Extracts commonalities from sites for the All files.
	Returns 0 for success. Adds "All" queues to sites. Updates the
	provenance info in the input dictionary. '''
	
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
				# Adding the "All" queue to the site
				d[cloud][site][All] = {param:all_d[cloud][site]}
				d[cloud][site][All].update({over:{}})

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
	

def compareQueues(dbDict,cfgDict,dbOverride=False):
	'''Compares the queue dictionary that we got from the DB to the one in the config files. Any changed/deleted
	queues are passed back. If dbOverride is set true, the DB is used to modify the config files rather than
	the default. Queues deleted in the DB will not be deleted in the configs.'''
	updDict = {}
	delDict = {}
	# Allow the reversal of master and subordinate dictionaries
	if dbOverride: dbDict, cfgDict = cfgDict, dbDict
	if dbDict == cfgDict: return updDict, delDict
	for i in dbDict:
		if not cfgDict.has_key(i):
			# Queues must be deleted in the configs.
			if not dbOverride:
				# If the queue is not in the configs, delete it.
				delDict[i]=dbDict[i]
				continue
		if dbDict[i]!=cfgDict[i]:
			# If the queue was changed in the configs, tag it for update.
			updDict[i]=cfgDict[i]

	# Return the appro
	return updDict, delDict
  
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

def buildDict():
	'''Build a copy of the queue dictionary from the configuration files '''

	confd={}
	# In executing files for variables, one has to put the variables in a contained, local context.
	locvars={}
	base = os.getcwd()
	# Loop throught the clouds in the base folder
	try:
		clouds = os.listdir(configs)
	except OSError:
		# If the configs folder is missing and this is the first thing run,
		# Reload this from the DB.
		# When SVN is in place, this should be replaced by a svn checkout.
		# We choose element 0 to get the first result. This hack will go away.
		makeConfigs(sqlDictUnpacker(loadSchedConfig())[0])
		clouds = os.listdir(configs)

	for cloud in clouds:
		# Add each cloud to the dictionary
		confd[cloud] = {}
		# Loop throught the sites in the present cloud folder
		sites = os.listdir(configs + os.sep + cloud)
		for site in sites:
			# If this is the All file, create another entry.
			if site.endswith(postfix):
				# Get rid of the .py
				s=site.rstrip(postfix)
				# Run the file for the dictionaries
				fname = configs + os.sep + cloud + os.sep + site
				# The appropriate dictionaries are placed in locvars
				execfile(fname,{},locvars)
				confd[cloud][s][param] = locvars[param]
				confd[cloud][s][over] = locvars[over]
			# Add each site to the cloud
			confd[cloud][site] = {}
			# Loop throught the queues in the present site folders
			queues = [i for i in os.listdir(configs + os.sep + cloud + os.sep + site) if i.endswith(postfix)]
			for q in queues:
				# Remove the '.py' 
				queue=q.rstrip(postfix)
				# Add each queue to the site
				confd[cloud][site][queue] = {}
				# Run the file to extract the appropriate dictionaries
				# As a clarification, the Parameters, Override and Enabled variable are created when the config python file is executed
				fname = configs + os.sep + cloud + os.sep + site + os.sep + q
				# The appropriate dictionaries are placed in locvars
				execfile(fname,{},locvars)
				# Feed in the configuration
				confd[cloud][site][queue][param] = locvars[param]
				confd[cloud][site][queue][over] = locvars[over] 
				confd[cloud][site][queue][enab] = locvars[enab]
				confd[cloud][site][queue][source] = dict([(key,'Config') for key in locvars[param] if key not in excl]) 				
				
	# Leaving the All parameters unincorporated
	os.chdir(base)
	return confd

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

def collapseDict(d):
	''' Collapses a nested dictionary into a flat set of queues '''
	out_d={}
	# Rip through the clouds
	for cloud in d:
		# And for each site
		for site in d[cloud]:
			# And for each queue
			for queue in d[cloud][site]:
				# Don't bother for an "All" queue
				if queue == All or site == All: continue
				# Get the parameter dictionary (vs the source or the overrides).
				# This is a symbolic link, not a duplication:
				p = d[cloud][site][queue][param]
				# So copy out the values into a new dictionary (except excluded ones)
				out_d[p[dbkey]] = dict([(key,p[key]) for key in p if key not in excl])
				# Make sure all the standard keys are present, even if not filled
				for key in standardkeys:
					if key not in p.keys(): out_d[p[dbkey]][key] = None
				# Add the overrides (except the excluded ones)
				for key in [i for i in d[cloud][site][queue][over] if i not in excl]: out_d[p[dbkey]][key] = d[cloud][site][queue][over][key]
				# Sanitization. Is this a good idea?
				for key in out_d[p[dbkey]]:
					if out_d[p[dbkey]][key] == 'None' or out_d[p[dbkey]][key] == '': out_d[p[dbkey]][key] = None
					if type(out_d[p[dbkey]][key]) is str and out_d[p[dbkey]][key].isdigit(): out_d[p[dbkey]][key] = int(out_d[p[dbkey]][key])
	# Return the flattened dictionary
	return out_d
				

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

if __name__ == "__main__":
	keydict={}
	def testDiff():
		
		for i in up_d:
			try:
				for k in m[i].keys():
					if k not in ['jdladd','releases']:
						if m[i][k] != n[i][k]:
							print i, k, m[i][k], n[i][k], type(m[i][k]), type(n[i][k])
			except KeyError:
				print '\n\n********************** %s was not found in the db\n\n' % i

	#cloudd = sqlDictUnpacker(unPickler('pickledSchedConfig.p'))
	# Load the present status of the DB, and describe a standard list of keys

	jdld = {}
	jdlListAdder(jdld)
	buildJdlFiles(jdld)
	newjdl=buildJdlDict()

	dbd, standardkeys = sqlDictUnpacker(loadSchedConfig())
	# Load the present config files
	configd = buildDict()
	
	# Compose the "All" queues for each site
	status = allMaker(configd)

	# Add the BDII information
	bdiiIntegrator(configd, dbd)
	# Now add ToA information to the whole shebang. No site-by-site as of yet.
	toaIntegrator(configd)

	# Compare the DB to the present built configuration
	up_d, del_d = compareQueues(collapseDict(dbd), collapseDict(configd))

	m,n=collapseDict(dbd),collapseDict(configd)
	u,d=compareQueues(collapseDict(dbd),collapseDict(configd))
	a=buildDeleteList(d,'atlas_pandameta.schedconfig')
	b=buildUpdateList(u,'atlas_pandameta.schedconfig')

	os.chdir(base_path)
		
		
