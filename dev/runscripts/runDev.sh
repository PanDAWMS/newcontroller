#! /bin/sh
export DBINTR=10
export PYTHONPATH=/data/atlpan/oracle/panda/monitor:/usr/lib/python2.5/site-packages
export LOCKFILE=/data/atlpan/panda/.schedconfigLock
export BASEPATH=/data/atlpan/panda/Dev_schedconfig
# Check the lock:
#if [ ! -e $LOCKFILE ]
#  then
#    :> $LOCKFILE
#  else
#    echo "Schedconfig update blocked by lockfile on host $HOSTNAME"|mail -s "** Schedconfig Update Blocked -- Lockfile **" schedconfig@gmail.com
#    exit 10
#fi


# Need grid setup
#source /afs/cern.ch/project/gd/LCG-share/current_3.2/external/etc/profile.d/grid-env.sh

echo "Setting panda dbtype to Oracle...."
export PANDA_DBTYPE=oracle

cd $BASEPATH/newController
svn update
python2.5 -i newController.py #--debug

#rm -rf $LOCKFILE
