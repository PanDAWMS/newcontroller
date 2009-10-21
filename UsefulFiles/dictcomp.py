import os, sys, pickle
from SchedulerUtils import utils

f=file('ref.p')
d2=pickle.load(f)
f.close()

utils.initDB()
utils.dictcursor().execute('select * from schedconfig')
d1=dict([(i['nickname'],i) for i in utils.dictcursor().fetchall()])
utils.closeDB()

for queue in d1:
	for val in d1[queue]:
		try:
			if d2[queue][val] != d1[queue][val]:
				print queue, d1[queue][val], d2[queue][val]
		except KeyError:
			print '%s is not present in the reference DB image.' % queue

