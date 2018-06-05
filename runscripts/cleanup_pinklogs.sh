#! /bin/sh
date
export LC_TIME=en_DK.UTF-8
echo "Cleaning time.... room service"
find /data/atlpan/oracle/panda/monitor/logs -mtime +15 -exec rm {} \;
echo "All done"
date