#! /usr/bin/env bash

# Setup of schedconfig updater. Run this script in its base directory as follows:
# source setup.sh

# It assumes that the path for the installation is at /data/atlpan/panda/schedconfigProd. If you want something different, please modify this installer and the run scripts -- without modifying them in SVN!

# It also assumes that panda/monitor and panda/autopilot are in /data/atlpan/panda/schedconfigProd (either local or linked).

# Then add the crons that are listed in crontabs.txt

BASEPATH=`pwd`
cd ..
ln -s $BASEPATH/runscripts/* .
ln -s autopilot/SchedulerUtils.py .
mkdir Backup
