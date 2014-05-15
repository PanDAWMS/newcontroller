##########################################################################################
# Tools for multicloud for T2D sites handling in newController                           #
#                                                                                        #
# Artem Petrosyan 10 Apr 2014                                                            #
# Artem.Petrosyan@cern.ch                                                                #
#                                                                                        #
##########################################################################################

from controllerSettings import *
from dbAccess import *

from datetime import datetime

class multicloudHandling:
    def getT1toT2DMatrix(self):
        if safety is 'on': utils.setTestDB()
        if setINTR:
            utils.setTestDB()
            print 'Using INTR Database'
        utils.initDB()
        print "Init DB"
        
        # fix me please!!! atlas_pandameta.""schedconfig"" fails because of processing in PandaMonitorUtils.py
        sql = "SELECT DISTINCT UPPER(s1.site) AS site_source, s2.site AS site_destination, s1.cloud AS cloud_source, s2.cloud AS cloud_destination, sonarlrgval FROM sites_matrix_data LEFT JOIN atlas_pandameta.""schedconfig"" s1 ON source=s1.nickname LEFT JOIN atlas_pandameta.""schedconfig"" s2 ON destination=s2.nickname WHERE source IN (SELECT nickname AS src FROM atlas_pandameta.""schedconfig"" s3 WHERE tier='%s' AND cloud<>'CMS' AND site<>'ARC-T2') AND destination IN (SELECT nickname AS dst FROM atlas_pandameta.""schedconfig"" s4 WHERE tier='%s' AND cloud<>'CMS') AND s1.cloud<>s2.cloud AND sonarlrgval>=%d ORDER BY site_destination, sonarlrgval DESC" % ('T1', 'T2D', multicloud_throughput_threshold_large)
        
        nrows = utils.dictcursor().execute(sql)
        if nrows > 0:
            # Fetch all the rows
            rows = utils.dictcursor().fetchall()
        
        # Close DB connection
        utils.endDB()
        
        return rows
        
#         data = {}
#         k = ''
#         for i in rows:
#             if k != i['SITE_DESTINATION']:
#                 k = i['SITE_DESTINATION']
#                 data.setdefault(i['SITE_DESTINATION'], {i['CLOUD_SOURCE']: i['SONARLRGVAL']})
#             else:
#                 data[i['SITE_DESTINATION']].update({i['CLOUD_SOURCE']: i['SONARLRGVAL']})
#         return data
    
    def updateMulticloud(self, matrix):
        dest = ''
        j = 1
        multicloud = ''
        
        for i in matrix:
            if i['SITE_DESTINATION'] != dest:
                #save if any then start building a new one
#                print 'site: ', dest
#                print 'multicloud: ', multicloud
                if dest != '':
                    self.InsertAndUpdate(dest, multicloud)
                
                dest = i['SITE_DESTINATION']
                j = 1
                multicloud = i['CLOUD_SOURCE']
                continue
                 
            if i['SITE_DESTINATION'] == dest and j < multicloud_number_of_sites_to_get:
                multicloud += ',' + i['CLOUD_SOURCE']
                j = j + 1
        
#        print 'site:', dest
#        print 'multicloud: ', multicloud
        self.InsertAndUpdate(dest, multicloud)
         
        return True
    
    def InsertAndUpdate(self, site, multicloud):
        if safety is 'on': utils.setTestDB()
        if setINTR:
            utils.setTestDB()
            print 'Using INTR Database'
        utils.initDB()
        print "Init DB"
        
        sql = "INSERT INTO multicloud_history (site, multicloud, last_update) VALUES ('%s', '%s', SYSDATE) " % (site, multicloud)
        sql1 = "UPDATE schedconfig SET multicloud='%s' WHERE site='%s'" % (multicloud, site)
        try:
            utils.dictcursor().execute(sql)
#            utils.dictcursor().execute(sql1)
        except:
            print "SQL failed: %s" % sql
#            print "SQL failed: %s" % sql1
        
        utils.commit()
        
        # Close DB connection
        utils.endDB()
    
    def Proceed(self):
        print datetime.now().replace(microsecond=0)
        print 'Get T1toT2D matrix'
        matrix = self.getT1toT2DMatrix()
#        print matrix
#         for i, v in matrix.iteritems():
# #            sv = sorted(v.items(), reverse=True)
#             sv = sorted(v, key=lambda i: int(v[i]), reverse=True)
#             print i
#             print v
#             print sv
        
        print datetime.now().replace(microsecond=0)
        print 'Calculate new value for multicloud field and update it, track changes'
        self.updateMulticloud(matrix)
        
        print datetime.now().replace(microsecond=0)
        print "Done"        
