##########################################################################################
# Simple utilities used in various newController components                              #
#                                                                                        #
# Alden Stradling 10 Oct 2009                                                            #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

import pickle, os

from controllerSettings import *

#----------------------------------------------------------------------#
# Utilities
#----------------------------------------------------------------------#

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

def reducer(l):
	''' Reduce the entries in a list by removing dupes'''
	return dict([(i,1) for i in l]).keys()

def noneChecker(a):
	for cloud in configd:
		for site in configd[cloud]:
			for queue in configd[cloud][site]:
				for i in configd[cloud][site][queue][param]:
					if configd[cloud][site][queue][param][i] == a:
						print type(configd[cloud][site][queue][param][i]), configd[cloud][site][queue][param][i]

def colChecker(a,d):
	for key in d:
		if d[key] == a:
			print type(a), key

def compDictLong(d1,d2,exclList=[]):
	for cloud in d1:
		for site in d1[cloud]:
			for queue in d1[cloud][site]:
				for i in d1[cloud][site][queue][param]:
					try:
						if d1[cloud][site][queue][param][i] != d2[cloud][site][queue][param][i]:
							if i not in exclList:
								print d1[cloud][site][queue][param][i], d2[cloud][site][queue][param][i], type(d1[cloud][site][queue][param][i]), type(d2[cloud][site][queue][param][i])	
					except KeyError:
						print 'No key %s in %s %s %s' % (i, cloud, site, queue)

def compDictColl(d1,d2,exclList=[]):
	for queue in d1:
		for i in d1[queue]:
			try:
				if d1[queue][i] != d2[queue][i]:
					if i not in exclList:
						print i, d1[queue][i], d2[queue][i], type(d1[queue][i]), type(d2[queue][i])	
			except KeyError:
				print 'No key %s in %s' % (i, queue)

def testDiff(mm,nn):
## 	for i in m:
## 		if type(m[i]) == dict: mm = collapseDict(m)
## 		else: mm = m
## 	for i in n:
## 		if type(n[i]) == dict: nn = collapseDict(n)
## 		else: nn = n

	for i in mm:
		try:
			for k in mm[i].keys():
				if k not in ['releases']:
					if mm[i][k] != nn[i][k]:
						if k=='jdladd':
							if mm[i][k][:mm[i][k].find('Queue')].strip() != nn[i][k][:nn[i][k].find('Queue')].strip():
								print i, k, mm[i][k][:mm[i][k].find('Queue')].strip(), nn[i][k][:nn[i][k].find('Queue')].strip(), type(mm[i][k]), type(nn[i][k])
						else:
							print i, k, mm[i][k], nn[i][k], type(mm[i][k]), type(nn[i][k])

		except KeyError:
			pass
