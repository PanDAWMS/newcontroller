# SchedConfig Controller
## Documentation
https://twiki.cern.ch/twiki/bin/view/PanDA/SchedConfigNewController

## Scheconfig parameter definitions 
https://twiki.cern.ch/twiki/bin/viewauth/AtlasComputing/SchedconfigParameterDefinitions

## Current schedules

### PanDA Controller Updates (aipanda161.cern.ch)
03,18,33,48 * * * *  /data/atlpan/panda/prod/runProd.sh 161 &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_schedconfigUpdate.txt
12 * * * * /data/atlpan/panda/prod/runSW.sh &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_swUpdate.txt
22 * * * * /usr/bin/python2.6 /data/atlpan/panda/prod/newController/runLogsCleaner.py &> /dev/null
### Artem Controller Updates
30 * * * * /data/atlpan/panda/prod/runNet.sh &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_networkUpdate.txt
50 0 * * 1 /data/atlpan/panda/prod/runMulticloud.sh &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_multicloudUpdate.txt

### PanDA Controller Updates (aipanda162.cern.ch)
08,23,38,53 * * * *  /data/atlpan/panda/prod/runProd.sh 161 &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_schedconfigUpdate.txt
32 * * * * /data/atlpan/panda/prod/runSW.sh &> /data/atlpan/panda/logs/schedconfig/run/${RANDOM}_swUpdate.txt
22 * * * * /usr/bin/python2.6 /data/atlpan/panda/prod/newController/runLogsCleaner.py &> /dev/null