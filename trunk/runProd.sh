#! /bin/sh
export PYTHONPATH=$PYTHONPATH:/data/atlpan/oracle/panda/monitor:/data/atlpan/panda/schedconfigProd:/data/atlpan/panda/schedconfigProd/autopilot:/data/atlpan/oracle/panda/monitor:/usr/lib/python2.5/site-packages
export LOCKFILE=/data/atlpan/panda/.schedconfigLock
export BASEPATH=/data/atlpan/panda/schedconfigProd
# Check the lock:
echo ${LOCKFILE}.${1}
if [ ! -e $LOCKFILE ]
  then
    :> $LOCKFILE
    :> ${LOCKFILE}.${1} 
  else
    echo "Schedconfig update blocked by lockfile on host $HOSTNAME"|mail -s "** Schedconfig Update Blocked -- Lockfile **" schedconfig@gmail.com
    exit 10
fi

# Need grid setup
#source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh

echo "Setting panda dbtype to Oracle...."
export PANDA_DBTYPE=oracle

cd $BASEPATH/newController
python2.5 newController.py

rm -rf $LOCKFILE*
