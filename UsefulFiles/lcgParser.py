##########################################################################################
# Tools for reading and parsing the LCG BDII                                             #
#                                                                                        #
# Alden Stradling 5 Mar 2010                                                            #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

import os, commands, re

#from miscUtils import *
#from controllerSettings import *
#from dictHandling import *

ceStr = 'CE:'
statusStr = '-CEStatus'
memStr = '-Memory'
smpStr = '-SMPSize'
maxcpuStr = '-MaxCPUTime'
tagStr = '-Tag'

allowedTags = {'offline-':'','tier0-':'Tier0-','topphys-':'TopPhys-','production-':'AtlasProduction-','atlasproduction-':'AtlasProduction-'}

cmd1 = 'lcg-info --vo atlas --list-ce --attrs CEStatus,Memory,SMPSize,MaxCPUTime,Tag'
cmd2 = 'lcg-info --vo VOMS:/atlas/Role=production --list-ce --attrs CEStatus,Memory,SMPSize,MaxCPUTime,Tag'

#list = commands.getoutput(cmd1).split(os.linesep)
#list.extend(commands.getoutput(cmd2).split(os.linesep))

f1=file('LCG_ALL_VOatlas.txt')
f2=file('LCG_ALL_VOMS.txt')

s = f1.read()
s += f2.read()

f1.close(); f2.close()

d={}

s=s.replace(' ','').replace(tagStr,'').replace(statusStr,'').replace(memStr,'').replace(smpStr,'').replace(maxcpuStr,'')
l=s.split(ceStr)
l=[i.strip() for i in l if len(i) > len(ceStr)]
for i in l:
	params = i.split()[:5]
	tags = i.split()[5:]
	ce = params[0].split(':')[0]
	jobmanager = params[0].split('/')[1].split('-')[1]
	queue = params[0].split('/')[1].split('-')[2]
