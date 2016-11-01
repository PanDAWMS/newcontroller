# SchedConfig Controller
## Documentation
https://twiki.cern.ch/twiki/bin/view/PanDA/SchedConfigNewController

## Scheconfig parameter definitions 
https://twiki.cern.ch/twiki/bin/viewauth/AtlasComputing/SchedconfigParameterDefinitions

## Current schedules
```
[atlpan@lxplus035 ~]$ acrontab -l
...
### SW listing
10 * * * * lxplus /afs/cern.ch/user/a/atlpan/bin/getSWonCVMFS.sh > /dev/null 2>&1
37 13 * * * aipanda045 /bin/echo pants &> /tmp/acron
### PanDA Controller Updates (Round Robin)
00,20,40 * * * * aipanda045 /data/atlpan/panda/prod/runProd.sh 45 &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_schedconfigUpdate.txt
02 * * * * aipanda045 /data/atlpan/panda/prod/runSW.sh &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_swUpdate.txt
12 * * * * aipanda045 /usr/bin/python2.6 /data/atlpan/panda/prod/newController/runLogsCleaner.py &> /dev/null
05,25,45 * * * * aipanda046 /data/atlpan/panda/prod/runProd.sh 46 &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_schedconfigUpdate.txt
17 * * * * aipanda046 /data/atlpan/panda/prod/runSW.sh &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_swUpdate.txt
27 * * * * aipanda046 /usr/bin/python2.6 /data/atlpan/panda/prod/newController/runLogsCleaner.py &> /dev/null
10,30,50 * * * * aipanda047 /data/atlpan/panda/prod/runProd.sh 47 &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_schedconfigUpdate.txt
32 * * * * aipanda047 /data/atlpan/panda/prod/runSW.sh &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_swUpdate.txt
42 * * * * aipanda047 /usr/bin/python2.6 /data/atlpan/panda/prod/newController/runLogsCleaner.py &> /dev/null
15,35,55 * * * * aipanda048 /data/atlpan/panda/prod/runProd.sh 48 &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_schedconfigUpdate.txt
47 * * * * aipanda048 /data/atlpan/panda/prod/runSW.sh &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_swUpdate.txt
57 * * * * aipanda048 /usr/bin/python2.6 /data/atlpan/panda/prod/newController/runLogsCleaner.py &> /dev/null
### Artem Controller Updates
10 * * * * aipanda045 /data/atlpan/panda/prod/runNet.sh &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_networkUpdate.txt
50 0 * * 1 aipanda045 /data/atlpan/panda/prod/runMulticloud.sh &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_multicloudUpdate.txt
```
