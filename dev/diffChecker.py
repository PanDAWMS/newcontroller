import os, sys, pickle

os.chdir('/Users/stradlin')
f1=open('dbd.py')
f2=open('configd.py')
f3=open('up_d.py')

d1=pickle.load(f1)
d2=pickle.load(f2)
d3=pickle.load(f3)

f1.close()
f2.close()
f3.close()

for i in d3:
	name = d3[i]['nickname']
	print name
	print 'Diffs starting with DBD'
	for j in d1[name]:
		if d1[name][j] != d2[name][j]:
			print j, d1[name][j], d2[name][j], type(d1[name][j]), type(d2[name][j])
	print 'Diffs starting with configd'
	for j in d2[name]:
		if d2[name][j] != d1[name][j]:
			print j, d2[name][j], d1[name][j], type(d2[name][j]), type(d1[name][j])
