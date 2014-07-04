#! /bin/sh
export PYTHONPATH=$PYTHONPATH:/data/atlpan/oracle/panda/monitor:/data/atlpan/panda/prod:/data/atlpan/panda/prod/autopilot:/usr/lib/python2.6/site-packages
export LOCKPATH=/afs/cern.ch/user/a/atlpan/public/schedconfig_dont_touch
export LOCKFILE=$LOCKPATH/.schedconfigLock
export BASEPATH=/data/atlpan/panda/prod
# Check the lock:
if [ ! -e $LOCKFILE ]
  then
    :> $LOCKFILE
  else
    echo "Schedconfig update blocked by lockfile on host $HOSTNAME"|mail -s "** Schedconfig Update Blocked -- Lockfile **" schedconfig@gmail.com
    exit 10
fi


# Need grid setup
#source /afs/cern.ch/project/gd/LCG-share/current_3.2/external/etc/profile.d/grid-env.sh

echo "Setting panda dbtype to Oracle...."
export PANDA_DBTYPE=oracle

cd $BASEPATH/newController
svn update
python2.6 -i newController.py --debug

rm -rf $LOCKFILE
