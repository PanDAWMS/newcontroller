##########################################################################################
# Simple utilities used in various newController components                              #
#                                                                                        #
# Alden Stradling 10 Oct 2009                                                            #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

import pickle, os, smtplib

from controllerSettings import *
from email.mime.text import MIMEText

#----------------------------------------------------------------------#
# Utilities
#----------------------------------------------------------------------#

def emailError(errorstring):
	'''Emails error messages to the appropriate address'''
	# Not using email.smtp.text and smtplib because of a python2.5 error. Please feel free to use the example code below for a working python version
	f = file(logPath)
	logStr = f.read()
	f.close()
	f = file('msgtemp','w')
	f.write('The schedconfig update code threw the following error: %s\n\n and update probably did not occur. Please check the error, or refer to the attached log file for further diagnostic help.\n\nThanks,\n\nSchedConfig\n\n%s' % (errorstring, logStr))
	f.close()
	os.system('cat msgtemp|mail -s "** Urgent -- Schedconfig Update Error **" %s' % errorEmail)

## 	f=file(logPath)
## 	logStr = f.read()
## 	f.close()
## 	logStr ='The schedconfig update code threw the following error: %s\n\n and update probably did not occur. Please check the error, or refer to the attached log file for further diagnostic help.\n\nThanks,\n\nSchedConfig\n\n%s' % (errorstring, logStr))
## 	msg = MIMEText(logStr)
## 	msg['Subject'] = '** Urgent -- Schedconfig Update Error **'
## 	msg['From'] = sourceEmail
## 	msg['To'] = errorEmail
## 	s = smtplib.SMTP()
## 	s.sendmail(sourceEmail, [errorEmail], msg.as_string())
## 	s.quit()
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
				if k not in ['releases','space']:
					if mm[i][k] != nn[i][k]:
						if k=='jdladd':
							mjdl=mm[i][k].split(); njdl=nn[i][k].split(); 
							mjdl=[n.strip() for n in mjdl]; njdl=[n.strip() for n in njdl]; 
							mjdl=' '.join(mjdl); njdl=' '.join(njdl)

							if mjdl != njdl:
								print
								print '==============================================='
								print 'Site %s, Queue %s' % (i, k)
								print '***********************************************'
								print '%s' %mjdl
								print '***********************************************'
								print '%s' % njdl
								print '***********************************************'
								print mm['cloud'], mm['site'], nn['cloud'],nn['site']
								print '==============================================='
								print
								
						else:
							print '->->-> ',
							print i, k, mm[i][k], nn[i][k], type(mm[i][k]), type(nn[i][k])

		except KeyError:
			pass


def unicodeConvert(x):
	'''General-purpose unicode converter'''
	if type(x) == str: return unicode(x)
	if type(x) == dict: return unicodeDictConvert(x)
	if type(x) == list: return unicodeListConvert(x)
	if type(x) == tuple: return unicodeTupleConvert(x)	
	if type(x) == int: return unicode(x)
	if type(x) == float: return unicode(x)
	return 0

def unicodeListConvert(l):
	'''Converts all strings in a list to unicode. No effort is made to detatch lists'''
	for n, i in enumerate(l):
		if type(i) == str: l[n] = unicode(i)
		elif type(i) == dict: unicodeDictConvert(i)
		elif type(i) == list: unicodeListConvert(i)
		elif type(i) == tuple: l[n] = unicodeTupleConvert(i)
		elif type(i) == int: l[n] = unicode(i)
		elif type(x) == float: l[n] = unicode(i)
	return 0
	
def unicodeDictConvert(d):
	'''Converts all strings in a dictionary to unicode. No effort is made to detatch dictionaries'''
	for i in d:
		if type(d[i]) == str: d[i] = unicode(d[i])
		elif type(d[i]) == dict: unicodeDictConvert(d[i])
		elif type(d[i]) == list: unicodeListConvert(d[i])
		elif type(d[i]) == tuple: d[i] = unicodeTupleConvert(d[i])
		if type(d[i]) == int: d[i] = unicode(d[i])
		if type(d[i]) == float: d[i] = unicode(d[i])
	return 0

def unicodeTupleConvert(t):
	'''Converts all strings in a tuple to unicode.'''
	new_tuple_list=[]
	for i in t:
		if type(t[i]) == str: new_tuple_list.append(unicode(i))
		elif type(i) == dict:
			unicodeDictConvert(i)
			new_tuple_list.append(i)
		elif type(t[i]) == list:
			unicodeListConvert(i)
			new_tuple_list.append(i)
		elif type(t[i]) == tuple: new_tuple_list.append(unicodeTupleConvert(i))
		if type(t[i]) == int: new_tuple_list.append(unicode(i))
		if type(t[i]) == float: new_tuple_list.append(unicode(i))
	return t

def unicodeEncode(x):
	'''General-purpose unicode decoder'''
	if type(x) == str: return x.decode(unidef,'ignore')
	if type(x) == dict: return unicodeDictEncode(x)
	if type(x) == list: return unicodeListEncode(x)
	if type(x) == tuple: return unicodeTupleEncode(x)	
	return 0

def unicodeListEncode(l):
	'''Encodes all strings in a list to UTF-8, or the default defined. No effort is made to detatch lists'''
	for n, i in enumerate(l):
		if type(i) == str: l[n] = i.encode(unidef,'ignore')
		elif type(i) == dict: unicodeDictEncode(i)
		elif type(i) == list: unicodeListEndcode(i)
		elif type(i) == tuple: l[n] = unicodeTupleEncode(i)
		elif type(i) == unicode: l[n] = i.encode(unidef,'ignore')
	return 0
	
def unicodeDictEncode(d):
	'''Encodes all strings in a dictionary to UTF-8, or the default defined. No effort is made to detatch dictionaries'''
	for i in d:
		if type(d[i]) == unicode: d[i] = d[i].encode(unidef,'ignore')
		elif type(d[i]) == dict: unicodeDictEncode(d[i])
		elif type(d[i]) == list: unicodeListEncode(d[i])
		elif type(d[i]) == tuple: d[i] = unicodeTupleEncode(d[i])
		elif type(d[i]) == str: d[i] = d[i].encode(unidef,'ignore')
	return 0

def unicodeTupleEncode(t):
	'''Encodes all strings in a tuple from unicode to UTF-8, or the default.'''
	new_tuple_list=[]
	for n, i in enumerate(t):
		if type(i) == unicode: new_tuple_list.append(i.encode(unidef,'ignore'))
		elif type(i) == dict:
			unicodeDictEncode(i)
			new_tuple_list.append(i)
		elif type(i) == list:
			unicodeListEncode(i)
			new_tuple_list.append(i)
		elif type(i) == tuple: new_tuple_list.append(unicodeTupleEncode(i))
		elif type(i) == str: new_tuple_list.append(i.encode(unidef,'ignore'))
	return t
