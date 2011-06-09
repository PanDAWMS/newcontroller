#! /usr/bin/env python
#######################################################
# Handles modification of queue data based on ToA and #
# BDII inputs.                                        #
# Based largely on existing code, cleaned up some.    #
# Alden Stradling (Alden.Stradling@cern.ch) 23 Jun 09 #
#######################################################

import os, commands, re

from miscUtils import *
from controllerSettings import *
from dictHandling import *

try:
	import dq2.info.TiersOfATLAS as ToA
except:
	print "Cannot import dq2.info.TiersOfATLAS, will exit"
	sys.exit(-1)

try:
	import lcgInfositeTool2 as lcgInfositeTool
except:
	print "Cannot import lcgInfositeTool, will exit"
	sys.exit(-1)

# Not used yet -- evaluate!
try:
	import fillDDMpaths
except:
	print "Cannot import fillDDMpaths, will exit"
	sys.exit(-1)

#----------------------------------------------------------------------#
# Integrators
#----------------------------------------------------------------------#
	  
def regionMap(confd):
	'''Using existing regional correlations to clouds, create a cloud lookup table'''
	region_map={}
	c=collapseDict(confd)
	for queue in c:
		if c[queue].has_key('region') and c[queue]['region'] and c[queue].has_key('cloud') and c[queue]['cloud']:
			if c[queue]['cloud'] in noAutoClouds: continue
			if not region_map.has_key(c[queue]['region']): region_map[c[queue]['region']] = [c[queue]['cloud']]
			else: region_map[c[queue]['region']].append(c[queue]['cloud'])
	for region in region_map:
		region_map[region] = reducer(region_map[region])
	return region_map
				
def loadToA(queuedefs):
	'''Acquires queue config information from ToA and updates the values we have. Should be run last. Overrides EVERYTHING else.'''
	fillDDMpaths.fillDDMpaths(queuedefs)
	return 0

def loadBDII():
	'''Loads LCG site definitions from the BDII, and dumps them in a file called lcgQueueUpdate.py in the local directory.
	This file is executed (even if generating it failed this time) and populated a dictionary of queue definitions, which is
	returned.'''
	osgsites={}
	print base_path
	if os.path.exists('%s/lcgLoad.py' % base_path):
		print 'Updating LCG sites from BDII'
		try:
			commands.getoutput('python2.5 %s/lcgLoad.py > lcgload.log' % base_path)
		except Exception, e:
			print 'Running lcgLoad.py failed:', e
			print 'Reusing existing lcgQueueUpdate.py'
		if os.path.exists('%s/lcgQueueUpdate.py' % base_path): execfile('lcgQueueUpdate.py')
		print 'LCG Initial Load Complete'
	else:
		loadlcg = 0
		print 'BDII Load Failed '
	unicodeConvert(osgsites)
	print len(osgsites)
	return osgsites

def keyCheckReplace(d,key,value):
	'''If nonfilled or nonexistent, fill a dict key without a KeyError'''
	if d.has_key(key):
		if not d[key]:
			d[key] = value
			return 1
		else: return 0
	else:
		d[key] = value
		return 1

def toaIntegrator(confd):
	''' Adds ToA information to the confd (legacy from Rod, incomplete commenting. Will enhance later.) '''
	print 'Running ToA Integrator'
	toaTime = False
	if toaDebug: print len(confd)
	ddmsites = ToA.getAllDestinationSites()
	for cloud in confd:
		if toaTime: print 'Cloud: %s, %s' % (cloud, time.asctime())
		if toaDebug: print len(confd[cloud])
		for site in confd[cloud]:
			if toaTime: print 'Site: %s, %s' % (site, time.asctime())
			if site is All: continue
			for queue in confd[cloud][site]:
				if toaTime: print 'Queue: %s, %s' % (queue, time.asctime())
				if queue is All: continue
				try:
					if toaDebug and confd[cloud][site][queue][param]['sysconfig'] == 'manual': print 'Skipping %s as a manual queue (%s, %s)' % (queue, cloud, site) 
					# Make ToA check the 'manual' sysconfig flag
					if confd[cloud][site][queue][param]['sysconfig'] == 'manual': continue
					if ToA and (not confd[cloud][site][queue][param].has_key('ddm') or (not utils.isFilled(confd[cloud][site][queue][param]['ddm']))):
						for ds in ddmsites:
							if toaTime: print 'GOCnames queue: %s, %s' % (site, time.asctime())							
							gocnames = ToA.getSiteProperty(ds,'alternateName')
							if not gocnames: gocnames=[]
							 # upper case for matching
							gocnames_up=[]
							for gn in gocnames:
								gocnames_up += [gn.upper()]  
							# If PRODDISK found use that
							if confd[cloud][site][queue][param]['site'].upper() in gocnames_up and ds.endswith('PRODDISK'):
								if toaDebug: print "Assign site %s to DDM %s" % ( confd[cloud][site][queue][param]['site'], ds )
								if toaTime: print '1 queue: %s, %s' % (site, time.asctime())							
								if keyCheckReplace(confd[cloud][site][queue][param], 'ddm', ds):
									confd[cloud][site][queue][source]['ddm'] = 'ToA'
								if keyCheckReplace(confd[cloud][site][queue][param], 'setokens', 'ATLASPRODDISK'):
									confd[cloud][site][queue][source]['setokens'] = 'ToA'
								# Set the lfc host 
								re_lfc = re.compile('^lfc://([\w\d\-\.]+):([\w\-/]+$)')
								if toaDebug: print "ds:",ds
								try:
									if toaTime: print '2 queue: %s, %s' % (site, time.asctime())							
									relfc=re_lfc.search(ToA.getLocalCatalog(ds))
									if relfc:
										lfchost=relfc.group(1)
										if keyCheckReplace(confd[cloud][site][queue][param], 'lfchost', lfchost):
											if toaDebug: print "Set lfchost to %s" % lfchost 
											confd[cloud][site][queue][source]['lfchost'] = 'ToA'
									else:
										if toaDebug: print " Cannot get lfc host for %s"%ds
								except:
									if toaDebug: print " Cannot get local catalog for %s"%ds
								# And work out the cloud
								if keyCheckReplace(confd[cloud][site][queue][param],'cloud', ''):
									confd[cloud][site][queue][source]['cloud'] = 'ToA'
								if not confd[cloud][site][queue][param]['cloud']:
									if toaDebug: print "Cloud not set for %s"%ds
									for cl in ToA.ToACache.dbcloud.keys():
										if ds in ToA.getSitesInCloud(cl):
											confd[cloud][site][queue][param]['cloud']= cl
											confd[cloud][site][queue][source]['cloud'] = 'ToA'

					# EGEE defaults
					if confd[cloud][site][queue][param]['sysconfig'] == 'manual': print 'How did we get here?'
					if toaTime: print '3 queue: %s, %s' % (site, time.asctime())
					if confd[cloud][site][queue][param]['region'] not in ['US','OSG']:

						# Use the pilot submitter proxy, not imported one (Nurcan non-prod) 
						# This was an original condition in Rod's version. I have removed it because there were
						# too many exceptions, and will only apply the noimport default where there is no previous
						# choice made.
						if keyCheckReplace(confd[cloud][site][queue][param],'proxy','noimport'):
							confd[cloud][site][queue][source]['proxy'] = 'ToA'
						if keyCheckReplace(confd[cloud][site][queue][param],'lfcpath','/grid/atlas/users/pathena'):
							confd[cloud][site][queue][source]['lfcpath'] = 'ToA'
						if keyCheckReplace(confd[cloud][site][queue][param],'lfcprodpath','/grid/atlas/dq2'):
							confd[cloud][site][queue][source]['lfcprodpath'] = 'ToA'
						if keyCheckReplace(confd[cloud][site][queue][param],'copytool','lcgcp'):
							confd[cloud][site][queue][source]['copytool'] = 'ToA'

						if confd[cloud][site][queue][param].has_key('ddm') and confd[cloud][site][queue][param]['ddm']:
							ddm1 = confd[cloud][site][queue][param]['ddm'].split(',')[0]
							if toaDebug: print 'ddm: using %s from %s' % (ddm1,confd[cloud][site][queue][param]['ddm'])
							# Set the lfc host 
							re_lfc = re.compile('^lfc://([\w\d\-\.]+):([\w\-/]+$)')

							if ToA:
								loccat = ToA.getLocalCatalog(ddm1)
								if toaTime: print 'Loccat queue: %s, %s' % (site, time.asctime())							
								if loccat:
									relfc = re_lfc.search(loccat)
									if relfc:
										lfchost = relfc.group(1)
										if toaDebug: print "ROD sets lfchost for %s %s" % (confd[cloud][site][queue][param]['ddm'],lfchost) 
										if keyCheckReplace(confd[cloud][site][queue][param], 'lfchost', lfchost):
											confd[cloud][site][queue][source]['lfchost'] = 'ToA'
									else:
										if toaDebug: print "Cannot get lfc host for %s" % ddm1

								if toaTime: print 'getSiteProperty queue: %s, %s' % (site, time.asctime())							
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
										confd[cloud][site][queue][param]['sepath'] = sepath + '/users/pathena'
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
						
	unicodeConvert(confd)
	print 'Finished ToA integrator'
	return

def bdiiIntegrator(confd,d,linfotool=None):
	'''Adds BDII values to the configurations, overriding what was there. Must be run after downloading the DB
	and parsing the config files.'''
	print 'Running BDII Integrator'
	out = {}
	# Load the queue names, status, gatekeeper, gstat, region, jobmanager, site, system, jdladd 
	print 'Loading BDII'
	bdict = loadBDII()
	len(bdict)
	# Moving on from the lcgLoad sourcing, we extract the RAM, nodes and releases available on the sites 
	if bdiiDebug: print 'Running the LGC SiteInfo tool'
	if not linfotool:
		linfotool = lcgInfositeTool.lcgInfositeTool()
	if bdiiDebug: print 'Completed the LGC SiteInfo tool run'

	region_map = regionMap(confd)

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
				# If a site is specified, go ahead (check the region_map for a cloud guess)
				try:
					if bdict[qn]['site']: c,s = region_map[bdict[qn]['site']],bdict[qn]['site']
				except KeyError:
					# This shouldn't happen -- but just in case.
					if bdict[qn]['site']: c,s = ndef,bdict[qn]['site']
				# If not, time to give up. BDII is hopelessly misconfigured -- best not to contaminate
				else: continue
				# If cloud doesn't exist, create it:
				if c not in confd:
					if bdiiDebug: print 'Creating cloud %s' % c
					confd[c]={}
				# If site doesn't yet exist, create it:
				if s not in confd[c]:
					if bdiiDebug: print 'Creating site %s in cloud %s as requested by BDII'  % (s,c)
					confd[c][s] = {}
					# Create it in the main config dictionary, using the standard keys from the DB (set in the initial import)
				print 'Creating queue %s in site %s and cloud %s as requested by BDII. This queue must be enabled by hand.' % (nickname, s, c)
				confd[c][s][nickname] = protoDict(nickname,{},sourcestr='Queue created by BDII',keys=standardkeys)
				confd[c][s][nickname][enab] = 'False'
				# Either way, we need to put the queue in without a cloud defined. 
		# If the queue doesn't actually exist, we need to add it.
		if c not in confd:
			if bdiiDebug: print 'Creating cloud %s' % c
			confd[c]={}
		# If site doesn't yet exist, create it:
		if s not in confd[c]:
			if bdiiDebug: print 'Creating site %s in cloud %s as requested by BDII'  % (s,c)
			confd[c][s] = {}
			# Create it in the main config dictionary, using the standard keys from the DB (set in the initial import)
		if not confd[c][s].has_key(nickname):
			confd[c][s][nickname] = protoDict(nickname,{},sourcestr='Queue created by BDII',keys=standardkeys)
			print 'Creating queue %s in site %s and cloud %s as requested by BDII. This queue must be enabled by hand.' % (nickname, s, c)

		# Before a "manual" check, update less crucial BDII params
		for key in ['localqueue','system','gatekeeper','jobmanager','site','region','gstat']:
			print 'gstat'
			confd[c][s][nickname][param][key] = bdict[qn][key]
			# Complete the sourcing info
			confd[c][s][nickname][source][key] = 'BDII'

		# Check for manual setting. If it's manual, DON'T TOUCH
		if not confd[c][s][nickname].has_key(param): continue
		if confd[c][s][nickname][param].has_key('sysconfig'):
			if confd[c][s][nickname][param]['sysconfig']:
				if confd[c][s][nickname][param]['sysconfig'].lower() == 'manual':
					if bdiiDebug: print 'Skipping %s -- sysconfig set to manual' % nickname
					continue
			if confd[c][s][nickname][param]['sysconfig'] == 'manual':
				print 'Real problem! This manual queue is being edited'

		# If jdladd is being set by BDII, check for an existing value first. Make sure it terminates with Queue, in any case.
		if not confd[c][s][nickname][param].has_key('jdladd'): confd[c][s][nickname][param]['jdladd']=''
		if not confd[c][s][nickname][param]['jdladd']: confd[c][s][nickname][param]['jdladd'] = bdict[qn]['jdladd']
		if not confd[c][s][nickname][param]['jdladd'].find('Queue') > 0: confd[c][s][nickname][param]['jdladd'] += '\n\nQueue\n'
		# For all the simple translations, copy them in directly.
		print 'Checking gstats'
		print len(bdict)
		for key in ['localqueue','system','gatekeeper','jobmanager','site','region','gstat']:
			confd[c][s][nickname][param][key] = bdict[qn][key]
			# Complete the sourcing info
			confd[c][s][nickname][source][key] = 'BDII'
		# For the more complicated BDII derivatives, do some more complex work
		confd[c][s][nickname][param]['queue'] = bdict[qn]['gatekeeper'] + '/'+ bdict[qn]['mgrprefix'] + '-' + bdict[qn]['jobmanager']
		#confd[c][s][nickname][param]['queue'] = bdict[qn]['gatekeeper'] + '/jobmanager-' + bdict[qn]['jobmanager']
		if not confd[c][s][nickname][param].has_key('jdl'): confd[c][s][nickname][param]['jdl'] = None
		if bdict[qn]['gatekeeper'] + '/jobmanager-' + bdict[qn]['jobmanager'] != confd[c][s][nickname][param]['jdl']:
			if bdiiDebug:
				print 'jdl mismatch!\n', bdict[qn], key, confd[c][s][nickname][param]['jdl'], 
		confd[c][s][nickname][param]['jdl'] = bdict[qn]['gatekeeper'] + '/'+ bdict[qn]['mgrprefix'] + '-' + bdict[qn]['jobmanager']
		#confd[c][s][nickname][param]['jdl'] = bdict[qn]['gatekeeper'] + '/jobmanager-' + bdict[qn]['jobmanager']
		confd[c][s][nickname][param]['nickname'] = nickname
		# Fill in sourcing here as well for the last few fields
		for key in ['queue','jdl','nickname']:
			confd[c][s][nickname][source][key] = 'BDII'
		
	unicodeConvert(confd)
	# All changes to the dictionary happened live -- no need to return it.
	print 'Finished BDII Integrator'
	return 0
