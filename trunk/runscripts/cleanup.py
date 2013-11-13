import os, sys

filesPerRun=3
runInterval=5
minutesPerDay=60*24
retainDays=-10
backupsKeep=filesPerRun*(minutesPerDay/runInterval)*retainDays
backupsLocation='/afs/cern.ch/user/a/atlpan/scratch0/schedconfig/prod/Backup/'

os.chdir(backupsLocation)
l=os.listdir(os.getcwd())
print len(l[:backupsKeep]),backupsKeep
for f in l[:backupsKeep]:
	os.remove(os.path.join(backupsLocation,f))

