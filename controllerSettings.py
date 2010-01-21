##########################################################################################
# Global settings for the newController system                                           #
#                                                                                        #
# Alden Stradling 5 Jan 2010                                                             #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

import os

# Debug Flags
genDebug = True
toaDebug = False
jdlDebug = False
bdiiDebug = False
dbReadDebug = False
dbWriteDebug = False
configReadDebug = False
configWriteDebug = False

# Global strings and lists
safety = 'on'
All = 'All'
ndef = 'Deactivated'
param = 'Parameters'
over = 'Override'
jdl = 'JDL'
source = 'Source'
enab = 'Enabled'
base_path = os.getcwd()
# Step back a layer in the path for the configs, and put them in the config SVN directory
cfg_path = base_path[:base_path.rfind(os.sep)] + os.sep + 'pandaconf' + os.sep
backupPath = cfg_path + 'Backup'
backupName = 'schedConfigBackup.pickle'
configs = cfg_path + os.sep + 'SchedConfigs'
jdlconfigs = cfg_path + os.sep + 'JDLConfigs'
postfix = '.py'
dbkey, dsep, keysep, pairsep, spacing = 'nickname', ' : ', "'", ',', '    '  # Standard python spacing of 4
shared, unshared = 'shared','unshared'
excl = ['status','lastmod','dn','tspace','_comment']
standardkeys = []
