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
    def getCloudsForMCU(self):
        if safety is 'on': utils.setTestDB()
        if setINTR:
            utils.setTestDB()
            print 'Using INTR Database'
        utils.initDB()
        print "Init DB"
        
        sql = "SELECT NAME FROM cloudconfig WHERE auto_mcu=1"
        
        nrows = utils.dictcursor().execute(sql)
        if nrows > 0:
            # Fetch all the rows
            rows = utils.dictcursor().fetchall()
        
        # Close DB connection
        utils.endDB()
        
        srows = ','
        for i in rows:
            srows += i['name'] + ','
        return srows
        
    def getT1toT2DMatrix(self):
        if safety is 'on': utils.setTestDB()
        if setINTR:
            utils.setTestDB()
            print 'Using INTR Database'
        utils.initDB()
        print "Init DB"
        
        # fix me please!!! atlas_pandameta.""schedconfig"" fails because of processing in PandaMonitorUtils.py
        sql = "SELECT DISTINCT UPPER(s1.site) AS site_source, s2.site AS site_destination, s2.nickname AS nickname_destination, s1.cloud AS cloud_source, s2.cloud AS cloud_destination, sonarlrgval, s2.multicloud AS multicloud_destination, s2.multicloud_append AS multicloud_append_destination, meas_date FROM sites_matrix_data LEFT JOIN atlas_pandameta.""schedconfig"" s1 ON source=s1.siteid LEFT JOIN atlas_pandameta.""schedconfig"" s2 ON destination=s2.siteid WHERE source IN (SELECT siteid AS src FROM atlas_pandameta.""schedconfig"" s3 WHERE (tier='%s' OR tier='%s') AND cloud<>'CMS' AND site<>'ARC-T2') AND destination IN (SELECT siteid AS dst FROM atlas_pandameta.""schedconfig"" s4 WHERE tier='%s' AND cloud<>'CMS' AND auto_mcu=1) AND s1.cloud<>s2.cloud AND meas_date>=(SYSDATE-3/24) ORDER BY nickname_destination, sonarlrgval DESC" % ('T0', 'T1', 'T2D')
        
        nrows = utils.dictcursor().execute(sql)
        if nrows > 0:
            # Fetch all the rows
            rows = utils.dictcursor().fetchall()
        
        # Close DB connection
        utils.endDB()
        
        return rows
    
    def getBestNT2DForT1(self, clouds):
        if safety is 'on': utils.setTestDB()
        if setINTR:
            utils.setTestDB()
            print 'Using INTR Database'
        utils.initDB()
        print "Init DB"
        
        # fix me please!!! atlas_pandameta.""schedconfig"" fails because of processing in PandaMonitorUtils.py
        sql = "SELECT DISTINCT UPPER(s1.site) AS site_source, s2.site AS site_destination, s1.cloud AS cloud_source, s2.cloud AS cloud_destination, sonarlrgval FROM sites_matrix_data LEFT JOIN atlas_pandameta.""schedconfig"" s1 ON source=s1.siteid LEFT JOIN atlas_pandameta.""schedconfig"" s2 ON destination=s2.siteid WHERE source IN (SELECT siteid AS src FROM atlas_pandameta.""schedconfig"" s3 WHERE (tier='%s' OR tier='%s') AND cloud<>'CMS' AND site<>'ARC-T2') AND destination IN (SELECT siteid AS dst FROM atlas_pandameta.""schedconfig"" s4 WHERE tier='%s' AND cloud<>'CMS' AND auto_mcu=1) AND s1.cloud<>s2.cloud ORDER BY site_source, sonarlrgval DESC" % ('T0', 'T1', 'T2D')
        
        nrows = utils.dictcursor().execute(sql)
        if nrows > 0:
            # Fetch all the rows
            rows = utils.dictcursor().fetchall()
        
        # Close DB connection
        utils.endDB()
        
        rows1 = []
        stest = ''
        cloudss = ''
        k = 0
        for i in rows:
            if stest != i['SITE_SOURCE'] and cloudss.find(i['CLOUD_SOURCE']) == -1 and clouds.find(i['CLOUD_SOURCE']) != -1:
                k = 0
                stest = i['SITE_SOURCE']
                clouds += ',' + i['CLOUD_SOURCE']
            
            if stest == i['SITE_SOURCE'] and k < multicloud_number_of_t2d_sites_to_get:
                rows1.append(i)
                k += 1
        
        return rows1
        
    def updateMulticloud(self, clouds, matrix, matrix1):
        dest = ''
        site_destination = ''
        multicloud = ''
        multicloud_old = ''
        multicloud_append = ''
        
        for i in matrix:
            if i['NICKNAME_DESTINATION'].find('ANALY_') != -1:
                continue
            if i['NICKNAME_DESTINATION'].find('_MCORE') != -1:
                continue
            if i['NICKNAME_DESTINATION'].find('_TEST') != -1:
                continue
            if i['NICKNAME_DESTINATION'].find('_Install') != -1:
                continue
            
            if i['NICKNAME_DESTINATION'] != dest:
                #save if any then start building a new one
#                print 'site: ', dest
#                print 'multicloud: ', multicloud
                if multicloud_append is not None:
#                    print dest, ': ', multicloud, '+', multicloud_append
                    if multicloud == '':
                        multicloud = multicloud_append
                    else:
                        multicloud_append_arr = multicloud_append.split(',')
                        for k in multicloud_append_arr:
                            if len(k) == 0:
                                continue
                            if multicloud.find(k) == -1:
                                multicloud += ',' + k
#                    print dest, ': ', multicloud
                
#                print dest, '!= "" and ', multicloud, '!= ""'
                if multicloud.count(',') < (multicloud_number_of_sites_to_get - 1):
#                    print dest, ': ', multicloud
                    for z in matrix1:
                        if site_destination == z['SITE_DESTINATION'] and clouds.find(z['CLOUD_SOURCE']) == -1:
                            multicloud += ',' + z['CLOUD_SOURCE']
                
                if dest != '':
                    if multicloud != multicloud_old:
                        self.InsertAndUpdate(dest, multicloud, multicloud_old)
                
                dest = i['NICKNAME_DESTINATION']
                site_destination = i['SITE_DESTINATION']
                multicloud = ''
                multicloud_old = i['MULTICLOUD_DESTINATION']
                multicloud_append = i['MULTICLOUD_APPEND_DESTINATION']
                continue
                 
            if i['NICKNAME_DESTINATION'] == dest and float(i['SONARLRGVAL']) >= multicloud_throughput_threshold_large and multicloud.count(',') < (multicloud_number_of_sites_to_get - 1) and multicloud.find(i['CLOUD_SOURCE']) == -1 and clouds.find(i['CLOUD_SOURCE']) != -1:
                if len(multicloud) == 0:
                    multicloud = i['CLOUD_SOURCE']
                else:
                    multicloud += ',' + i['CLOUD_SOURCE']
        
#        print 'site:', dest
#        print 'multicloud: ', multicloud
        if multicloud_append is not None:
            if multicloud == '':
                multicloud = multicloud_append
            else:
                multicloud_append_arr = multicloud_append.split(',')
                for k in multicloud_append_arr:
                    if len(k) == 0:
                        continue
                    if multicloud.find(k) == -1:
                        multicloud += ',' + k
        
        if multicloud.count(',') < (multicloud_number_of_sites_to_get - 1):
            for z in matrix1:
                if site_destination == z['SITE_DESTINATION'] and multicloud.count(',') < (multicloud_number_of_sites_to_get - 1) and multicloud.find(z['CLOUD_SOURCE']) == -1 and clouds.find(z['CLOUD_SOURCE']) != -1:
                    multicloud += ',' + z['CLOUD_SOURCE']
        if multicloud != multicloud_old:
            self.InsertAndUpdate(dest, multicloud, multicloud_old)
         
        return True
    
    def InsertAndUpdate(self, nickname, multicloud, multicloud_old):
        if safety is 'on': utils.setTestDB()
        if setINTR:
            utils.setTestDB()
            print 'Using INTR Database'
        utils.initDB()
        print "Init DB"
        
        sql = "INSERT INTO multicloud_history (site, multicloud, last_update) VALUES ('%s', '%s', SYSDATE) " % (nickname, multicloud_old)
        try:
            utils.dictcursor().execute(sql)
        except:
            print "SQL failed: %s" % sql
            # Close DB connection
            utils.endDB()
            return False
        
        sql1 = "UPDATE schedconfig SET multicloud='%s' WHERE nickname='%s'" % (multicloud, nickname)
        try:
            utils.dictcursor().execute(sql1)
        except:
            print "SQL failed: %s" % sql1
            # Close DB connection
            utils.endDB()
            return False
         
        utils.commit()
        
        # Close DB connection
        utils.endDB()
        
        return True
    
    def Proceed(self):
        print datetime.now().replace(microsecond=0)
        
        print 'Get clouds where auto_mcu=1'
        clouds = self.getCloudsForMCU()
        
        print 'Get T1toT2D matrix'
        matrix = self.getT1toT2DMatrix()
        
        print 'Get best N T2D for each T1'
        matrix1 = self.getBestNT2DForT1(clouds)
        
        print datetime.now().replace(microsecond=0)
        print 'Calculate new value for multicloud field and update it, track changes'
        self.updateMulticloud(clouds, matrix, matrix1)
        
        print datetime.now().replace(microsecond=0)
        print "Done"        
