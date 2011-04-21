import commands, re, os

class lcgInfositeTool:

	CEram = {}
	CEmaxcpu = {}

	def __init__(self):
		self.debug = False		
		self.CEtags={}		# dict to hold ce:[list of tags]
		self.CEctags={}		# keep prod and tier cache tags separate 
		self.StartStr = 'VO-atlas-'	# Fiters ATLAS-specific tags	
		self.CESep = '%'		# Standard separation between CE and tags
		self.PortSep = ':'		# Separates CE and port
		self.RelSep = '&'		# Delineates releases
		self.CacheSep = '.'		# Release separators (like 15.3.4)
		self.CacheNum = 3       # Below this number of periods -- base release. Above -- cache.
		self.TagSep = '-'		# Get ATLAS SW tags per CE
		self.minlen = 15        # Filter _UNDEF_ and other crap to avoid dictionary collisions

		# Run the collection algorithms in init.
		self.buildSWtags(test=False)
		self.buildRAMlist(test=False)

	def buildSWtags(self,test=False):
		cmd= 'lcg-info --vo atlas --list-ce --sed --attr Tag'
		if not test:
			cst, cedata1 = commands.getstatusoutput(cmd)
			if cst != 0:
				print "Bail out of LCG load. %s failed."%cmd
				return 1
			# run it again because RAL does not accept plain atlas VO
		cmd= 'lcg-info --vo VOMS:/atlas/Role=production --list-ce --sed --attr Tag'
		if not test:
			cst, cedata2 = commands.getstatusoutput(cmd)
			if cst != 0:
				print "Bail out of LCG load. %s failed."%cmd
				return 1
			# Merge the two
			cedata=cedata1+os.linesep+cedata2

		if test:
			f=file('linfo.txt')
			cedata = f.read()
			f.close()

		# This whole section depends on nested list comprehensions. Review them if you are confused as to what's going on here.

		# This replaces some regex handling which has proven to be fragile wrt the evolving ATLAS SW scheme, and allows for nonstandard
		# US release names in the BDII
   
		# Split the string on line separations, and get rid of blanks
		lines = [i for i in cedata.split(os.linesep) if i]

		# Build a dictionary with the CEs and the raw release string list as key and value pairs 
		tags = dict([(i.split(self.PortSep)[0],i.split(self.CESep)[1].split(self.RelSep)) for i in lines if len(i.split(self.CESep)[1].split(self.RelSep)) > self.minlen])
		origtags = tags

		# Removing redundant release listings: for each CE gatekeeper, pass the list into a dictionary and take the keys,
		# guaranteeing that there are no repeats.
		rtags = dict([(i,sorted(dict([(j,1) for j in tags[i]]).keys())) for i in tags])

		# Removing VO-atlas- and any trailing i686 or opt stuff, and remove all non-ATLAS material 
		tags = dict([(i,sorted([self.TagSep.join(j.split(self.StartStr)[1].split(self.TagSep)[:2]) for j in rtags[i] if j.startswith(self.StartStr)])) for i in rtags])
		rtags = dict([(i,sorted([self.TagSep.join(j.split(self.StartStr)[1].split(self.TagSep)[2:]) for j in rtags[i] if j.startswith(self.StartStr)])) for i in rtags])

		# Getting the release caches sorted into their own dictionary
		ctags = dict([(i,sorted([j for j in tags[i] if j.count(self.CacheSep) >= self.CacheNum])) for i in tags if i.count(self.CacheSep) > 0])

		# Base release filtering -- lots of repeats...
		tags = dict([(i,sorted([j.split(self.TagSep)[1] for j in tags[i] if j.count(self.TagSep)])) for i in tags])

		# ... so we do repeat removal again and trim off excess cache numbers
		tags = dict([(i,sorted(dict([(self.CacheSep.join(j.split(self.CacheSep)[:self.CacheNum]),1) for j in tags[i]]).keys())) for i in tags])

		# Remove all caches that don't look like Athena releases
		tags = dict([(i,sorted([j for j in tags[i] if j.count(self.CacheSep)])) for i in tags])

        # Put in 'Conditions' pseudo-release based on pre-stripped tags
		for ce in tags.keys():
		  if origtags.has_key(ce) \
		      and 'VO-atlas-poolcond' in origtags[ce]:
		    tags[ce] += ['Conditions']	  
		# Allow output
		self.CEtags, self.CEctags = tags, ctags
		
		# Temporary hack until MWT2 gets their system consistent
		try:
			self.CEtags['uct2-grid6.mwt2.org'] = self.CEtags['uct2-grid6.uchicago.edu']
			self.CEctags['uct2-grid6.mwt2.org'] = self.CEctags['uct2-grid6.uchicago.edu']
			self.CEtags['iut2-grid6.mwt2.org'] = self.CEtags['iut2-grid6.iu.edu']
			self.CEctags['iut2-grid6.mwt2.org'] = self.CEctags['iut2-grid6.iu.edu']
		except KeyError:
			pass
		return 0

	def buildRAMlist(self,test=False):
		# A local top BDII is much faster,10sec	
		if test:
			# To test run this by hand and save output in tags.lis
			cmd = 'cat tags.lis'
		else:
			# Buggy
			#			cmd = 'lcg-infosites -v 2 --vo atlas ce'
			cmd = 'lcg-info --vo atlas --list-ce --attrs Memory,SMPSize,MaxCPUTime --sed'
			
		print "Running: "+cmd
		cst, cedata = commands.getstatusoutput(cmd)

		if cst != 0:
			print "Bail out of LCG load. %s failed."%cmd
			return 1

		# Split to CE hostname and the rest 
		lines = cedata.split('\n')


		re_isint=re.compile('^\d+$')
		# key will be host-queue
		re_ceq = re.compile('([-\w.]+):\d+/\w+-\w+-(\w+)')
	
		self.CEram={}
		self.CEmaxcpu={}

		for line in lines:
			totram, smp = 0, 0
			sline = line.split('%')
			lline = len(sline)
			if lline == 4:
				ce =	sline[0]			
				if re_isint.search(sline[1]):
					totram = int(sline[1])
				if re_isint.search(sline[2]):
					smp	= int(sline[2])
				if re_isint.search(sline[3]):
					maxcpu	= int(sline[3])
				if totram and smp and smp != 0:
 #				 print ce,totram,smp
					ram = max(1024,int(totram/smp))
				else:
					ram = 0
			else:
				print line
#			print ce,ram,maxcpu

			receq=re_ceq.search(ce)
			if receq:
				ceq = receq.group(1)+'-'+receq.group(2)
				if ceq not in self.CEram:
					self.CEram[ceq]=ram
				if ceq not in self.CEmaxcpu:
				 # info in minutes 
					self.CEmaxcpu[ceq]=maxcpu*60
			else:
				print 'Cannot extract ceq from ',ce

	def getSWtags(self,ce):
	 # For ce hostname, return a list of shortened SW tags
	 if ce in self.CEtags.keys():
		 return self.CEtags[ce]
	 else:
		 print 'ERROR: %s not found in tag list'%ce
		 return []

	def getSWctags(self,ce):
		# For ce hostname, return a list of shortened SW tags
		if ce in self.CEctags.keys():
			return self.CEctags[ce]
		else:
			print 'ERROR: %s not found in tag list'%ce
			return []
										

	def getRAM(self,ce,queue):
	 # For ce host-jobmanager-queue, return RAM
		ceq = ce+'-'+queue
		if ceq in self.CEram.keys():
			return self.CEram[ceq]
		else:
			print 'ERROR: %s not found in RAM list'%ceq
			return 0

	def getMaxcpu(self,ce,queue):
		# For ce host-jobmanager-queue, return Maxcpu
		ceq = ce+'-'+queue
		if ceq in self.CEmaxcpu.keys():
			return self.CEmaxcpu[ceq]
		else:
			print 'ERROR: %s not found in Maxcpu list'%ceq
			return 0

	def getCEs(self):
		return self.CEtags.keys()

	def getCEcs(self):
		return self.CEctags.keys()


if __name__ == '__main__':
	itool=lcgInfositeTool()		
	ces=['wipp-ce.weizmann.ac.il','ce1.triumf.ca','grid-ce4.desy.de','snowpatch-hep.westgrid.ca','lcgce01.cpp.ualberta.ca','ce.gina.sara.nl']
	for ce in ces:
		tags = itool.getSWtags(ce)
		print 'CE: %s has: %s'%(ce,', '.join(tags))
		tags = itool.getSWctags(ce)
		print 'CE: %s has: %s'%(ce,', '.join(tags))
			 

	ram = itool.getRAM('ce1.triumf.ca','atlas')
	print 'CE has: %d'%ram
	ram = itool.getRAM('svr021.gla.scotgrid.ac.uk','q3d')
	print 'CE has: %d'%ram
	maxcpu=itool.getMaxcpu('svr021.gla.scotgrid.ac.uk','q3d')
	print 'CE has maxcpu: %d'%maxcpu
