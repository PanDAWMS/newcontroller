##########################################################################################
# Global settings for the newController system                                           #
#                                                                                        #
# Alden Stradling 5 Jan 2010                                                             #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

import os

# Debug Flags
genDebug = False
toaDebug = False
jdlDebug = False
bdiiDebug = False
dbReadDebug = False
dbWriteDebug = False
configReadDebug = False
configWriteDebug = False

# Global strings and lists

# If safety is on, nothing is written to the DB.
safety = 'off'

# Widely used strings

All = 'All' # Standard name for All files
ndef = 'Deactivated' # Standard name for cloud=NULL queues
param = 'Parameters' # Name for the parameters dictionary in a queue spec
over = 'Override' # Name for the override dictionary in a queue spec
jdl = 'JDL' # The parameters dictionary for a JDL specification (no need for override)
source = 'Source' # The name for a sources dictionary (names the provenance of a setting in the parameters dictionary for a queue
enab = 'Enabled' # Specifies that a queue has been enabled.

# Sets the present path as the primary -- allows portability, but the script has to be run from its home directory.
base_path = os.getcwd()

# Step back a layer in the path for the configs, and put them in the config SVN directory
cfg_path = base_path[:base_path.rfind(os.sep)] + os.sep + 'pandaconf' + os.sep
# Paths for backup files
backupPath = cfg_path + 'Backup'
backupName = 'schedConfigBackup.pickle'
# Config file path specifications
configs = cfg_path + os.sep + 'SchedConfigs'
jdlconfigs = cfg_path + os.sep + 'JDLConfigs'
postfix = '.py'
# jdlkey is the jdllist table primary key, and dbkey is the schedconfig table primary key.
jdlkey, dbkey, dsep, keysep, pairsep, spacing = 'name', 'nickname', ' : ', "'", ',', '    '  # Standard python spacing of 4
shared, unshared = 'shared','unshared'
# These are the DB fields that should never be modified by the controller -- fixed by hand using curl commands.
excl = ['status','lastmod','dn','tspace','_comment']
# This list is global, and is populated (during initial DB import) by the list of columns that the schedconfig table contains. 
standardkeys = []
