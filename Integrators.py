import os, commands, re

from miscUtils import *
from controllerSettings import *

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

# Not used yet -- evaluate!
try:
	import fillDDMpaths
except:
	print "Cannot import fillDDMpaths, will exit"
	sys.exit(-1)

#----------------------------------------------------------------------#
# Integrators
#----------------------------------------------------------------------#
	  
# To be completed!!
def loadToA(queuedefs):
	'''Acquires queue config information from ToA and updates the values we have. Should be run last. Overrides EVERYTHING else.'''
	fillDDMpaths.fillDDMpaths(queuedefs)
	return 0

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

# Rewrite this to be more efficient -- it needs to parse the ddmsites once into a dictionary, then do matchmaking.
def toaIntegrator(confd):
	''' Adds ToA information to the confd (legacy from Rod, incomplete commenting. Will enhance later.) '''
	print 'Running ToA Integrator'
	if toaDebug: print len(confd)
	for cloud in confd:
		if toaDebug: print len(confd[cloud])
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
