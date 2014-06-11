#! /bin/sh
export PYTHONPATH=$PYTHONPATH:/data/atlpan/oracle/panda/monitor:/data/atlpan/panda/prod:/data/atlpan/panda/prod/autopilot:/data/atlpan/oracle/panda/monitor:/usr/lib/python2.5/site-packages
export LOCKPATH=/afs/cern.ch/user/a/atlpan/public
export LOCKFILE=$LOCKPATH/.swLock
export BASEPATH=/data/atlpan/panda/prod
# Check the lock:
if [ ! -e $LOCKFILE ]
  then
    :> $LOCKFILE
    :> ${LOCKFILE}.${1} 
  else
    echo "Schedconfig update blocked by lockfile on host $HOSTNAME"|mail -s "** Schedconfig Update Blocked -- Lockfile **" schedconfig@gmail.com
    exit 10
fi

# Need grid setup
#source /afs/cernch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh

echo "Setting panda dbtype to Oracle...."
export PANDA_DBTYPE=oracle

cd $BASEPATH/newController
svn update --accept theirs-full
python2.5 newController.py --sw

rm -rf $LOCKFILE*
