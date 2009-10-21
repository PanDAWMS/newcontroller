import re, string

try:
  import dq2.info.TiersOfATLAS as ToA
except:
  print "fillDDMpaths: Cannot import dq2.info.TiersOfATLAS, will exit"
  sys.exit(-1)


def fillDDMpaths(inspec):
  print "In fillDDMpaths for ",inspec['site']
  if inspec['nickname'].startswith('ANALY_'):
    setokens_try=['ATLASSCRATCHDISK','ATLASLOCALGROUPDISK']
#    setokens_try=['ATLASUSERDISK']
  else:
    setokens_try=['ATLASPRODDISK','ATLASMCDISK']

  ddmsites = ToA.getAllDestinationSites()

 # Build a dict of gocnames per site
  gnds={}
  for ds in ddmsites:
    gocnames = ToA.getSiteProperty(ds,'alternateName')
    if not gocnames: gocnames=[]
   # upper case for matching
    gocnames_up=[]
    for gn in gocnames:
      gocnames_up+=[gn.upper()]  
    gnds[ds]=gocnames_up

 # Regexp to extract token from srm endpoint
  re_srm2_ep=re.compile('(token:(\w+):(srm://[\w.\-]+):\d+/srm/managerv\d\?SFN=)(/[\w.\-/]+)/?$')

# These list will be put into spec
  setokens=[]
  sepaths=[]
  ses=[]
  ddm=[]
 # Loop over desired tokens
  for ttok in setokens_try:
   # then over ddm sites
    for ds in gnds.keys():
#      print ds,  gnds[ds]
      if inspec['site'].upper() in gnds[ds] or inspec['site'].upper() in ds :
       # Extract token from endpoint
        srm_ep = ToA.getSiteProperty(ds,'srm')
#        print ds,srm_ep
        if not srm_ep: continue
        resrm2_ep=re_srm2_ep.search(srm_ep)
        if resrm2_ep:
          ftok=resrm2_ep.group(2)
#          print "Found token: %s"%ftok
         # but be careful of ToA duplicates 
          if ftok==ttok and ttok not in setokens:
            sepath=resrm2_ep.group(4)
            se=resrm2_ep.group(1)
#            print sepath
#            print se
            setokens+=[ttok]
            sepaths+=[sepath]
            ses+=[se]
            ddm+=[ds]

  sepath=contractPath(sepaths)
# This should not be here. Multiple ses are handled manually.
#  se=contractPath(ses)

 # Make the token in the se equal to the first in setokens
 # Only set first if multiple se`s are set
 # This is the default, for panda/ganga(?) if user sets no token
  se =  inspec['se']
  if len(setokens)>=1:
    deftok=setokens[0]
    seold = se
   # to extract token 
    re_srm2_ep=re.compile('(token:(\w+):(srm://[\w.\-]+):\d+/srm/managerv\d\?SFN=)$')
    resrm2_ep = re_srm2_ep.search(seold)
    if resrm2_ep:
      oldtok = resrm2_ep.group(2)
      se = seold.replace(oldtok,deftok)
  
  print 'Would set spec[setokens]=',','.join(setokens)
  print 'Would set spec[sepath]=',sepath
  print 'Would set spec[seprodpath]=',sepath
  print 'Would set spec[se]=',se
  print 'Would set spec[ddm]=',','.join(ddm)
 # but only if at least one matching token was found
  if len(setokens)>=1:
    inspec['setokens']=','.join(setokens)
    inspec['sepath']=sepath
    inspec['seprodpath']=sepath
    inspec['se']=se
    inspec['ddm']=','.join(ddm)

def contractPath(paths):
 # list of paths to be contracted in [...,...,...] notation   
 # find first part in common
 # Do not worry about common end part

  if len(paths)==0:
    return ''
  elif len(paths)==1:
    return paths[0]

# by character
#  first=paths[0]
#  for i in range(1,len(first)+1):
#    snip=first[0:i]
#    got=0
#    for path in paths:  
#      if path.find(snip) == 0: got+=1
#    if got == len(paths):        
#      common=first[0:i]

# by path segment      
  firstsp=paths[0].split('/')
  print firstsp
  for i in range(0,len(firstsp)):
    match=0
    for path in paths:
      if path.split('/')[i]==firstsp[i]:
        match+=1
    if match<len(paths):
      common='/'.join(firstsp[0:i])+'/'
      break

  print common
  lc=len(common)
  dpaths=[]
  for path in paths:
      print path[lc:]
      dpaths+=[path[lc:]]
  cpath=common+'['+','.join(dpaths)+']'    
  return cpath
  
if __name__ == '__main__':
  spec={}
#  spec['site']='GRIF-LAL'
#  spec['nickname']='ANALY_GRIF-LAL'
#  spec['site']='IN2P3-LPC'
#  spec['nickname']='ANALY_LPC'
#  spec['site']='GRIF-SACLAY'
#  spec['nickname']='ANALY_GRIF-IRFU'
#  spec['site']='IN2P3-CC'
#  spec['nickname']='ANALY_LYON'
  spec['site']='GRIF-LAL'
  spec['nickname']='ANALY_GRIF-LAL'
#  spec['site']='IN2P3-CC'
#  spec['nickname']='ANALY_LYON'
#  spec['site']='BEIJING-LCG2'
#  spec['nickname']='ANALY_BEIJING'
#  spec['site']='FZK-LCG2'
#  spec['nickname']='ANALY_FZK'
  spec['se'] = 'token:ATLASUSERDISK:srm://ccsrm.in2p3.fr:8443/srm/managerv2?SFN='  
  fillDDMpaths(spec)
  print spec
