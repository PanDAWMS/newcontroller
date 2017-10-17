#! /usr/bin/env python
#######################################################
# Handles backups as pickle files and such            #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 18 Feb 10 #
#######################################################

from controllerSettings import *

hbList = [i for i in sorted(os.listdir(hotBackupPath)) if i.endswith('.gz')][hotBackups:]
lbList = [i for i in sorted(os.listdir(longBackupPath)) if i.endswith('.gz')][keptBackups:]

for i in hbList:
    os.system('rm -f %s' % hotBackupPath + i)
    print('rm %s' % hotBackupPath + i)
for i in lbList:
    os.system('rm -f %s' % longBackupPath + i)
    print('rm %s' % longBackupPath + i)
