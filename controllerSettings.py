##########################################################################################
# Global settings for the newController system #
#                                                                                        #
# Alden Stradling 5 Jan 2010                                                             #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

import os

# Default unicode encoding
unidef='utf-8'

# Debug Flags
genDebug = False
toaDebug = False
jdlDebug = False
svnDebug = False
agisDebug = True
bdiiDebug = False
pickleDebug = False
delDebug = False
lesserDebug = True
dbReadDebug = False
dbWriteDebug = False
allMakerDebug = False
configReadDebug = False
configWriteDebug = False

# Global strings and lists

# SVN repositories
confrepo = 'svn+ssh://svn.cern.ch/reps/pandaconf/trunk' 

# If safety is on, nothing is written to the DB.
safety = 'off'

# Widely used strings
All = 'All' # Standard name for All files
Conditions = 'Conditions' # Standard meta-tag for the presence of Conditions data at a site
ndef = 'Deactivated' # Standard name for cloud=NULL queues
param = 'Parameters' # Name for the parameters dictionary in a queue spec
over = 'Override' # Name for the override dictionary in a queue spec
jdl = 'JDL' # The parameters dictionary for a JDL specification (no need for override)
source = 'Source' # The name for a sources dictionary (names the provenance of a setting in the parameters dictionary for a queue
enab = 'Enabled' # Specifies that a queue has been enabled or disabled -- a variable set to true or false.
svn = '.svn' # Allows for filtering of SVN directories
rel = 0  # rel is the first element of the rel/cmt block in swHandling
cmt = 1  # cmt is the second element of the rel/cmt block in swHandling
cmtDashes = 3 # The number of dashes that characterize a real CMT specification in installedsw

# Sets the present path as the primary -- allows portability, but the script has to be run from its home directory.
base_path = os.getcwd()

# Step back a layer in the path for the configs, and put them in the config SVN directory
cfg_path = base_path[:base_path.rfind(os.sep)] + os.sep + 'pandaconf/'
cmp_path = base_path[:base_path.rfind(os.sep)] + os.sep + 'pandaconfRef/'

# Paths for backup files
scratchPath = '/afs/cern.ch/user/a/atlpan/scratch0/schedconfig/prod/'
runLogPath = '/afs/cern.ch/user/a/atlpan/scratch0/schedconfig/logs/'
hotBackupPath = base_path[:base_path.rfind(os.sep)] + os.sep + 'Backup' + os.sep
longBackupPath = scratchPath + 'Backup' + os.sep
backupSQLName = 'schedConfigBackup.sql'
backupCSVName = 'schedConfigBackup.csv'
volatileSQLName = 'schedConfigStatus.sql'
volatileCSVName = 'schedConfigStatus.csv'
lastVolatiles = 10
hotBackups = 90
keptBackups = 1500
keptRunLogs = 15000
# Paths for run logs (email notification)
logPath = '/afs/cern.ch/user/a/atlpan/scratch0/schedconfig/logs/'
errorFile = '/tmp/pandaUpdateErrors.log'
errorFileJDL = '/tmp/pandaUpdateErrorsJDL.log'

# Maximum value of site max cpu time
maxMaxTime = 864000 # Corresponds to 10 days of run

# Default email address for failure notifications

errorEmail = 'schedconfig@gmail.com'
sourceEmail = 'atlpan@mail.cern.ch'

# Config file path specifications
configs = cfg_path + os.sep + 'SchedConfigs'
jdlconfigs = cfg_path + os.sep + 'JDLConfigs'
lesserconfigs = cfg_path + os.sep + 'Others'
postfix = '.py'

# jdlkey is the jdllist table primary key, and dbkey is the schedconfig table primary key.
jdlkey, dbkey, dsep, keysep, pairsep, spacing = 'name', 'nickname', ' : ', "'", ',', '    '  # Standard python spacing of 4
shared, unshared = 'shared','unshared'

# These are the DB fields that are required not to be null, along with defaults. 
nonNull={'name':'default','system':'unknown','site':'?','nqueue':'0','nodes':'0','queuehours':'0','memory':'0', 'maxtime':'0', 'space':'0','statusoverride':'offline'}

# These are the DB fields that should never be modified by the controller -- fixed by hand using curl commands.
excl = ['status','lastmod','dn','tspace','comment_','space','nqueue','sysconfig','multicloud','statusoverride'] # nqueues takes care of a typo
nonexistent = ['nqueues']
timestamps = ['lastmod','tspace'] # Fields that are explicitly timestamps, and are as such harder to update in the DB
excl_nonTimestamp = [i for i in excl if i not in timestamps + nonexistent] # List of items to back up

# These fields are to be consistent across siteids
siteid_consistent = ['cloud','ddm','lfchost','se','memory','maxtime','space','retry','cmtconfig','setokens','seprodpath','glexec','priorityoffset','allowedgroups','defaulttoken','queue','localqueue','validatedreleases','accesscontrol','copysetup','maxinputsize','cachedse','allowdirectaccess','lfcregister','countrygroup','availablecpu','pledgedcpu']

# Standard mappings for legacy software tags in the BDII:

tagsTranslation = {'offline':'AtlasOffline','production':'AtlasProduction','tier0':'AtlasTier0','topphys':'TopPhys','wzbenchmarks':'WZBenchmarks'}

# Clouds that don't auto-populate from BDII

noAutoClouds = ['NDGF','OSG','US']

# This list is global, and is populated (during initial DB import) by the list of columns that the schedconfig table contains. 
standardkeys = []

# Software release base assumptions:
nBaseReleaseSep = 3 # The ASSUMED number of base release identifying fields (like 15.4.5). Longer (point release or cache) names are
# permitted, but are considered "caches".
baseReleaseSep = '.' # The separator used for release naming (like 15.4.5)
virtualQueueGatekeeper = 'to.be.set' # The value that virtual queues have instead of gatekeeper

# Length of a standard BDII entry

# DB Override flag -- Default is False
dbOverride = False

bdiiOverride = False
toaOverride = False
