#! /usr/bin/env bash

for i in 1 2 3; do
	if [ -e /afs/cern.ch/user/a/atlpan/Trigger/trigger.txt ]; 
		then 
		rm /afs/cern.ch/user/a/atlpan/Trigger/trigger.txt; 
		/afs/cern.ch/user/a/atlpan/schedconfigProd/runProd.sh; 
	fi
        if [ -e /afs/cern.ch/user/a/atlpan/Trigger/swtrigger.txt ];
                then
                rm /afs/cern.ch/user/a/atlpan/Trigger/swtrigger.txt;
                /afs/cern.ch/user/a/atlpan/schedconfigProd/runSW.sh;
        fi

	sleep 25
done
