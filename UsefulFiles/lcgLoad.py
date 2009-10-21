#!/bin/env python
import commands, re, sys, os
from SchedulerUtils import utils

# Site, region specs
regions = {
    'yu' : 'Serbia/Montenegro',
    'br' : 'Brazil',
    'sg' : 'Singapore',
    'jp' : 'Japan',
    'au' : 'Australia',
    'uk' : 'UK',
    'it' : 'Italy',
    'ru' : 'Russia',
    'su' : 'Russia',
    'cern.ch' : 'CERN',
    'cz' : 'CzechR',
    'tw' : 'Taiwan',
    'bg' : 'Bulgaria',
    'ie' : 'Ireland',
    'tr' : 'Turkey',
    'si' : 'Slovenia',
    'il' : 'Israel',
    'hu' : 'Hungary',
    'cy' : 'Cyprus',
    'ro' : 'Romania',
    'cn' : 'China',
    'ch' : 'Switzerland',
    'es' : 'Spain',
    'hr' : 'Hungary',
    'fr' : 'France',
    'fr.cgg.com' : 'France',
    'gr' : 'Greece',
    'nl' : 'Holland',
    'my' : 'Malaysia',
    'pl' : 'Poland',
    'se' : 'Sweden',
    'pt' : 'Portugal',
    'de' : 'Germany',
    'pk' : 'Pakistan',
    'ca' : 'Canada',
    'sk' : 'Slovakia',
    'at' : 'Austria',
    'edu' : 'US',
    'gov' : 'US',
    'com' : 'US',
    'org' : 'US',
    'int' : 'International',
    'ma' : 'Morocco',
    'cl' : 'Chile',
    }
def getRegion(str):
    rlist = [ 'cern.ch', 'fr.cgg.com', ]
    rlist += regions.keys()
    for r in rlist:
        pat = re.compile('.*%s$' % r)
        mat = pat.match(str)
        if mat:
            return regions[r]
    return ''

specs = {
    'cnaf.infn.it' : 'INFN-CNAF',
    'cern.ch' : 'CERN-PROD',
    'erciyes.edu.tr' : 'ERCIYES',
    'cu.edu.tr' : 'CUKUROVA',
    'metu.edu.tr' : 'METU',
    'ulakbim.gov.tr' : 'TR-10-ULAKBIM',
    'nipne.ro' : 'NIPNE',
    'pic.es' : 'PIC',
    'ific.uv.es' : 'IFIC-LCG2',
    'srce.hr' : 'SRCE',
    'irb.hr' : 'IRB',
    'ultralight.org' : 'Caltech',
    'univ-bpclermont.fr' : 'AUVERGRID',
    'inp.demokritos.gr' : 'DEMOKRITOS',
    'hep.ntua.gr' : 'HEPNTUA',
    'sara.nl' : 'SARA-MATRIX',
    'wcss.wroc.pl' : 'WCSS',
    'fhg.de' : 'FHG',
    'triumf.ca' : 'TRIUMF',
    'ncp.edu.pk' : 'NCP',
}
#    'lrz-muenchen.de' : 'LRZ-Munich',


## Read CE data
try:
    print "Doing lcg-info with LCG_GFAL_INFOSYS=%s" % os.environ['LCG_GFAL_INFOSYS']
except:
    print "Doing lcg-info. LCG_GFAL_INFOSYS not defined"
cst, cedata1 = commands.getstatusoutput('lcg-info --vo atlas --list-ce --sed')
# Just for RAL
cst, cedata2 = commands.getstatusoutput('lcg-info --vo VOMS:/atlas/Role=production --list-ce --sed')
cedata=cedata1+'\n'+cedata2
print cedata
if cst != 0 or len(cedata) < 1000:
    print "Bail out of LCG load. lcg-infosites failed."
    sys.exit(0)
fh = open("lcgQueueUpdate.py","w")
celist = cedata.split('\n')
if len(celist) < 10:
    print "Bail out of LCG load. lcg-infosites has <10 entries."
    sys.exit(0)
cedict = {}
pat = re.compile('(.*):([0-9]+)/(.*)-(.*)-(.*)')
for ce in celist:
  if ce == '': continue
  mat = pat.match(ce)
  gatekeeper = '?'
  jobmanager = '?'
  port = '?'
  jdladd = ''
  batchsys = '?'
  if mat:
      gatekeeper = mat.group(1)
      port = mat.group(2)
      mgrprefix = mat.group(3)
      batchsys = mat.group(4)
      localqueue = mat.group(5)
      jobmanager = "%s-%s-%s" % ( mgrprefix, batchsys, localqueue )
      jdladd = "globusrsl = (queue=%s)\\n" % localqueue
            
      if port == '2119':
        queuename = "%s/%s" % ( gatekeeper, jobmanager )
      else:
        queuename = "%s:%s/%s" % ( gatekeeper, port, jobmanager )
      cedict[queuename] = {}
      ced = cedict[queuename]
      ced['fullname'] = ce
      ced['gatekeeper'] = gatekeeper
      ced['jobmanager'] = jobmanager
      ced['queue'] = queuename
      ced['jdladd'] = jdladd
      ced['batchsys'] = batchsys
      ced['localqueue'] = localqueue
      ced['mgrprefix'] = mgrprefix
      ced['status'] = 'online'
  else:
      print "No match to",ce

## Get known CEs from DB
utils.initDB()
query = "select * from schedconfig"
utils.dictcursor().execute(query)
drows = utils.dictcursor().fetchall()
nrows = len(drows)
print "Queues read from DB", nrows
setspecs = []
for r in drows:
    queue = r['queue']
    jobmgr = '?'
    if utils.isFilled(queue):
        pat = re.compile('.*/(.*)')
        mat = pat.match(queue)
        if mat: jobmgr = mat.group(1)
    jdladd = r['jdladd']
    cename = queue
    if utils.isFilled(jdladd):
        pat = re.compile('.*\(queue=(.*)\)')
        mat = pat.search(jdladd)
        if mat:
            localqueue = mat.group(1)
            cename = "%s-%s" % ( queue, localqueue )
    r['cename'] = cename
    if cedict.has_key(cename):
        #print "MATCHED DB queue", cename
        pass
    else:
        #print "nomatch DB queue", cename
        ## Does this gatekeeper appear at all?
        for ced in cedict:
            if r['gatekeeper'] == cedict[ced]['gatekeeper'] and jobmgr == cedict[ced]['jobmanager']:
                print "%s: GK/jobmanager found but queue attributes changed %s. Marking DB entry as obsolete" % ( r['nickname'], r['cename'] )
                spec = {}
                spec['nickname'] = r['nickname']
                spec['status'] = 'obsolete'
                setspecs.append(spec)
if len(setspecs) > 0:
    utils.replaceDB('schedconfig',setspecs,key='nickname')

## Get data from BDII via ldap
if os.environ.has_key('LCG_GFAL_INFOSYS'):
    topbdii=os.environ['LCG_GFAL_INFOSYS']
else:
    topbdii='lcg-bdii.cern.ch:2170'
    
#commands.getoutput('ldapsearch -x -h lcg-bdii.cern.ch -p 2170     -b "o=grid" -x "GlueCEAccessControlBaseRule=VO:atlas" > ldap-atlas.dat')
commands.getoutput('ldapsearch -x -H ldap://%s -b "o=grid" -x "(|(GlueCEAccessControlBaseRule=VO:atlas)(GlueCEAccessControlBaseRule=VOMS:/atlas/Role=production))" > ldap-atlas.dat'%topbdii)
ldaph = open('ldap-atlas.dat')
ldapdat = ldaph.read()
ldaph.close()
ldapdat = ldapdat.replace('\n ','')
ldapdat = ldapdat.split('\n')

for ced in cedict:
    if ced.find('blah') == -1:
        ## Get BDII data
        patstr = cedict[ced]['fullname']
        patstr = patstr.replace('.','\.')
        patstr = "^\#[\s]*atlas,[\s]*.*%s[^ ]*,[\s]*([^,]+).*$" % patstr
        pat = re.compile(patstr)
        matched = 0
        qname = ''
        cename = ''
        region = getRegion(cedict[ced]['gatekeeper'])
        
        for l in ldapdat:
            mat = pat.match(l)
            if mat:
                cename = mat.group(1)
                qname = "%s-%s" % ( cename, cedict[ced]['localqueue'] )
#                print "In BDII:",qname, l
                matched = 1
        if not matched:
            print "Queue %s : %s not in BDII" % ( ced, qname )
            print cedict[ced]
            continue

        site = ''
        for s in specs:
            patstr = '^.*%s/.*' % s
            pat = re.compile(patstr)
            mat = pat.match(ced)
            if mat:
                site = specs[s]
        if not utils.isFilled(site): site = cename
        if not utils.isFilled(region):
            print "Error: don't know region for", cename, cedict[ced]['gatekeeper']
#            sys.exit(0)
        sitespec = "  'site' : '%s',\n  'region' : '%s',\n  'gstat' : '%s',\n" % ( site, region, cename )
        # If this queue is already defined -- same full queue name -- use the existing nickname
        matched = ''
        for r in drows:
            if r['cename'] == ced and r['gatekeeper'] == cedict[ced]['gatekeeper']: matched = r['nickname']
        pat = re.compile('^([^\.]+)')
        mat = pat.match(cedict[ced]['gatekeeper'])
        if mat:
            machine = mat.group(1)
            qname = "%s-%s-%s" % ( cename, machine, cedict[ced]['localqueue'] )
            nickname = "%s-%s" % ( qname, cedict[ced]['batchsys'] )
        else:
            print "no match to ",cedict[ced]['gatekeeper']
            nickname = "%s-%s" % ( qname, cedict[ced]['batchsys'] )
        if utils.isFilled(matched):
            #print "Queue %s exists in DB with nickname %s" % ( ced, matched )
            if matched != nickname:
                #print "   DIFFERENT to preferred nickname %s. Will use existing nickname" % nickname
                nickname = matched
        else:
            print "Queue %s name %s not found in DB" % ( ced, nickname )
        # Update
        if utils.isFilled(qname):
            dbqname = '%s/%s-%s' % ( cedict[ced]['gatekeeper'], cedict[ced]['mgrprefix'], cedict[ced]['batchsys'] )
            fh.write("\nosgsites['%s'] = {\n  'gatekeeper' : '%s',\n%s  'jobmanager' : '%s',\n  'localqueue' : '%s',\n  'system' : 'lcg-cg',\n  'jdladd' : '%s',\n  'status' : '%s',\n}" % ( qname, cedict[ced]['gatekeeper'], sitespec, cedict[ced]['batchsys'], cedict[ced]['localqueue'], cedict[ced]['jdladd'], cedict[ced]['status']))
        else:
            print "ERROR: queue data not found in ldap for", ced
utils.endDB()
fh.write('\n')
fh.close()

## Read SE data
#sedata = commands.getoutput('lcg-infosites --vo ATLAS se')
