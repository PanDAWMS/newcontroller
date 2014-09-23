#! /bin/sh
export PYTHONPATH=$PYTHONPATH:/data/atlpan/oracle/panda/monitor:/data/atlpan/panda/prod:/data/atlpan/panda/prod/autopilot:/data/atlpan/oracle/panda/monitor:/usr/lib/python2.6/site-packages
export LOCKPATH=/afs/cern.ch/user/a/atlpan/public/schedconfig_dont_touch
export LOCKFILE=$LOCKPATH/.netLock
export BASEPATH=/data/atlpan/panda/prod


# Need grid setup
source /afs/cern.ch/project/gd/LCG-share/current_3.2/external/etc/profile.d/grid-env.sh

echo "Setting panda dbtype to Oracle...."
export PANDA_DBTYPE=oracle

cd $BASEPATH/newController
svn update
python2.6 newController.py --multicloud

