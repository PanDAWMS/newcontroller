MERGE INTO ATLAS_PANDAMETA.schedconfig USING DUAL ON ( ATLAS_PANDAMETA.schedconfig.nickname='AGLT2-condor' ) WHEN MATCHED THEN UPDATE SET g
atekeeper=:gatekeeper,site=:site,lfcpath=:lfcpath,sysconfig=:sysconfig,wntmpdir=:wntmpdir,copytool=:copytool,tmpdir=:tmpdir,copyprefix=:copyprefix,envsetup=:envsetup,system=:system
,datadir=:datadir,python_path=:python_path,seprodpath=:seprodpath,cloud=:cloud,ddm=:ddm,lfcprodpath=:lfcprodpath,nodes=:nodes,status=:status,lfchost=:lfchost,tags=:tags,appdir=:app
dir,siteid=:siteid,setokens=:setokens,dq2url=:dq2url,jdl=:jdl,name=:name,region=:region,cmd=:cmd,errinfo=:errinfo,queue=:queue,jdladd=:jdladd,environ=:environ,special_par=:special_
par,sepath=:sepath,se=:se WHEN NOT MATCHED THEN INSERT (schedconfig.gatekeeper,schedconfig.site,schedconfig.lfcpath,schedconfig.sysconfig,schedconfig.wntmpdir,schedconfig.copytool,
schedconfig.tmpdir,schedconfig.copyprefix,schedconfig.envsetup,schedconfig.system,schedconfig.datadir,schedconfig.python_path,schedconfig.seprodpath,schedconfig.cloud,schedconfig.d
dm,schedconfig.lfcprodpath,schedconfig.nodes,schedconfig.status,schedconfig.lfchost,schedconfig.tags,schedconfig.appdir,schedconfig.siteid,schedconfig.setokens,schedconfig.dq2url,s
chedconfig.jdl,schedconfig.name,schedconfig.region,schedconfig.cmd,schedconfig.errinfo,schedconfig.queue,schedconfig.jdladd,schedconfig.environ,schedconfig.special_par,schedconfig.
sepath,schedconfig.se,schedconfig.nickname) VALUES (:gatekeeper,:site,:lfcpath,:sysconfig,:wntmpdir,:copytool,:tmpdir,:copyprefix,:envsetup,:system,:datadir,:python_path,:seprodpat
h,:cloud,:ddm,:lfcprodpath,:nodes,:status,:lfchost,:tags,:appdir,:siteid,:setokens,:dq2url,:jdl,:name,:region,:cmd,:errinfo,:queue,:jdladd,:environ,:special_par,:sepath,:se,:nickna
me)
with values  {'gatekeeper': 'gate01.aglt2.org', 'site': 'GreatLakesT2', 'lfcpath': '', 'sysconfig': 'manual', 'wntmpdir': '/tmp', 'copytool': 'lcg-cp2', 'tmpdir': '/tmp', 'copypref
ix': '^srm://head01.aglt2.org', 'envsetup': 'source /afs/atlas.umich.edu/OSGWN/setup.sh', 'system': 'osg', 'datadir': '/atlas/data08/OSG/DATA', 'python_path': '/atlas/data08/OSG/AP
P/atlas_app/python/python32bit-2.4/bin/python', 'seprodpath': '/pnfs/aglt2.org/atlasproddisk', 'cloud': 'US', 'ddm': 'AGLT2_PRODDISK', 'environ': 'APP=/atlas/data08/OSG/APP TMP=/tm
p DATA=/atlas/data08/OSG/DATA', 'nodes': '118', 'status': 'online', 'lfchost': 'lfc.aglt2.org', 'tags': '', 'appdir': '/atlas/data08/OSG/APP/atlas_app/atlas_rel', 'siteid': 'AGLT2'
, 'nickname': 'AGLT2-condor', 'setokens': 'ATLASPRODDISK', 'dq2url': '', 'jdl': 'AGLT2-condor', 'name': 'default', 'region': 'US', 'cmd': 'condor_submit -verbose %s', 'errinfo': ''
, 'queue': 'gate01.aglt2.org/jobmanager-condor', 'jdladd': 'Environment = "APP=/atlas/data08/OSG/APP TMP=/tmp DATA=/atlas/data08/OSG/DATA clusterid=$(Cluster)"\n\nQueue\n', 'lfcpro
dpath': '/grid/atlas/dq2', 'special_par': '(maxWallTime=4000)', 'sepath': '', 'se': 'token:ATLASPRODDISK:srm://head01.aglt2.org:8443/srm/managerv2?SFN='}
