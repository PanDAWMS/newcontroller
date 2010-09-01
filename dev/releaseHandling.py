#! /usr/bin/env python

###########################################
# releaseHandler.py                       #
#                                         #
# Creates release lists and adds them to  #
# the installedsw table                   #
#                                         #
# Alden Stradling  31 Aug 2010            #
# Alden.Stradling@cern.ch                 #
##########################################

import os, sys

from controllerSettings import *
from miscUtils import *
from dictHandling import *
from configFileHandling import *
from svnHandling import *

try:
	import lcgInfositeTool2 as lcgInfositeTool
except:
	print "Cannot import lcgInfositeTool, will exit"
	sys.exit(-1)

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
	unicodeConvert(osgsites)
	return osgsites

def releaseLister(confd,rellist):
	'''Extracts the list of releases from BDII, matches them to queues in schedconfig,
	then passes out a final list of releases and caches'''

	# The input dictionary needs to be a dictionary
	if not type(confd) == dict:
		print 'ReleaseLister: You didn\'t pass in a dictionary. Try again.'
		sys.exit()
	
	bdict = loadBDII()
	# Moving on from the lcgLoad sourcing, we extract the RAM, nodes and releases available on the sites 
	if bdiiDebug: print 'Running the LGC SiteInfo tool'
	linfotool = lcgInfositeTool.lcgInfositeTool()
	if bdiiDebug: print 'Completed the LGC SiteInfo tool run'


	gatekeepers=reducer(linfotool.getCEs()+linfotool.getCEcs())
	siteids={}
	clouds={}
	
	for cloud in [i for i in confd.keys() if i is not ndef]:
		for site in [i for i in confd[cloud].keys() if i is not ndef]:
			for queue in [i for i in confd[cloud][site].keys() if (i is not All and i is not ndef)]:
				try:
					gk=confd[cloud][site][queue][param]['gatekeeper']
				except KeyError:
					pass
					
				# Check to see if the gatekeeper is even listed in BDII
				if gk not in gatekeepers:
					# Or perhaps the beginning of the queue (which can substitute for the gatekeeper for some queues) 
					try:
						gk=confd[cloud][site][queue][param]['queue'].split(os.sep)[0]
					except KeyError:
						pass
					if gk not in gatekeepers:
						# If not, we just go on to the next queue.
						continue
				# This is deliberately single-valued -- siteids that associate to multiple gatekeepers will malfunction, on purpose
				if confd[cloud][site][queue][param].has_key('siteid') \
				   and siteids.has_key(confd[cloud][site][queue][param]['siteid']) \ 
				   and siteids[confd[cloud][site][queue][param]['siteid']] != gk:
					print 'There\'s more than one gatekeeper for siteid %s: %s, %s'
					% (confd[cloud][site][queue][param]['siteid'],gk,siteids[confd[cloud][site][queue][param]['siteid']]) \
					  print 'Overwriting.'
					# Check for non-null siteid
				if confd[cloud][site][queue][param].has_key('siteid') and confd[cloud][site][queue][param]['siteid']:
					siteids[confd[cloud][site][queue][param]['siteid']] = gk
					clouds[confd[cloud][site][queue][param]['siteid']] = cloud

				
					

	for siteid in siteids: 
		releases,caches=linfotool.getSWtags(gk),linfotool.getSWctags(gk)
		if len(rels):
			for c in caches:
				cache=c.replace('production','AtlasProduction').replace('tier0','AtlasTier0').replace('topphys','TopPhys').replace('wzbenchmarks','WZBenchmarks') 
				release='.'.join(tag.split('-')[1].split('.')[:3])
				idx = '%s_%s' % (siteid,cache)
				rellist[idx]=dict([('siteid',siteid),
								   ('cloud',clouds[siteid]),
								   ('release',release),
								   ('cache',cache)])

				rellist[idx]=dict([('siteid',siteid),
								   ('cloud',clouds[siteid]),
								   ('release',release),
								   ('cache','')])
