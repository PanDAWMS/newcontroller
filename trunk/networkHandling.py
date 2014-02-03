##########################################################################################
# Tools for handling network measurements sites matrix in newController                  #
#                                                                                        #
# Artem Petrosyan 1 Aug 2013                                                             #
# Artem.Petrosyan@cern.ch                                                                #
#                                                                                        #
##########################################################################################

import os, pickle, cPickle

from controllerSettings import *
from dbAccess import *

import urllib

try:
    import json
except:
    import simplejson as json

from datetime import datetime

def getNormalizedValue(val):
    if val >= max_mbs:
        val = 0.5
    else:
        val = round((val/max_mbs) * w_norm, 2)
    return val

class networkHandling():

    def getData(self):
        '''Downloads atlassites src-dst matrix from AGIS as a dictionary'''
        
        try:
            data = json.load(urllib.urlopen(agis_sites_matrix_url))
        except IOError:
            f = file('sitesmatrix.p')
            data = cPickle.load(f)
            f.close()
        
        data1 = {}
        for i in data:
            if not 'snrsmlval' in i.keys():
                i.update({u'snrsmlval': u'null'})
#             else:
#                 i['snrsmlval'] = getNormalizedValue(i['snrsmlval'])
            
            if not 'snrsmldev' in i.keys():
                i.update({u'snrsmldev': u'null'})
            
            if not 'snrmedval' in i.keys():
                i.update({u'snrmedval': u'null'})
#             else:
#                 i['snrmedval'] = getNormalizedValue(i['snrmedval'])
            
            if not 'snrmeddev' in i.keys():
                i.update({u'snrmeddev': u'null'})
            
            if not 'snrlrgval' in i.keys():
                i.update({u'snrlrgval': u'null'})
#             else:
#                 i['snrlrgval'] = getNormalizedValue(i['snrlrgval'])
            
            if not 'snrlrgdev' in i.keys():
                i.update({u'snrlrgdev': u'null'})
            
            if not 'psnravgval' in i.keys():
                i.update({u'psnravgval': u'null'})
#             else:
#                 i['psnravgval'] = getNormalizedValue(i['psnravgval'])
            
            if not 'xrdcpval' in i.keys():
                i.update({u'xrdcpval': u'null'})
#             else:
#                 i['xrdcpval'] = getNormalizedValue(i['xrdcpval'])
            
            if i['snrsmlval'] == u'null' and i['snrmedval'] == u'null' and i['snrlrgval'] == u'null' and i['psnravgval'] == u'null' and i['xrdcpval'] == u'null':
                continue  
            data1.setdefault(i['src'] + '_to_' + i['dst'], i)
        return data1

    def getMapping(self):
        '''Downloads pandaresources for atlassites for mapping from AGIS as a dictionary'''
        
        try:
            data = json.load(urllib.urlopen(agis_pandaresource_url))
        except IOError:
            f = file('pandaresource.p')
            data = cPickle.load(f)
            f.close()
        
        return data
    
    def buildPandaQueueMatrix(self, sites_matrix, pandaresources):
        '''Returns pandasites src-dst matrix as a dictionary'''
        
        pandasites_matrix = {}
        for p1 in pandaresources:
            for p2 in pandaresources:
                t1 = p1['panda_queue_name'][0:6]
                t2 = p2['panda_queue_name'][0:6]
                if (t1 == 'ANALY_' and t2 != 'ANALY_') or (t1 != 'ANALY_' and t2 == 'ANALY_'):
                    continue
                if p1['panda_queue_name'] != p2['panda_queue_name']:
                    k1 = p1['atlas_site'] + '_to_' + p2['atlas_site']
                    k2 = p1['panda_queue_name']+ '_to_' + p2['panda_queue_name']
                    if k1 in sites_matrix.keys() and not k2 in pandasites_matrix.keys():
                        i = sites_matrix[k1]
                        pandasites_matrix.setdefault(k2, 
                                                    {
                                                     'src': p1['panda_queue_name'],
                                                     'dst': p2['panda_queue_name'],
                                                     'src_atlassite': p1['atlas_site'],
                                                     'dst_atlassite': p2['atlas_site'],
                                                       
                                                     'psnravgval': i['psnravgval'],
                                                     'snrlrgdev': i['snrlrgdev'],
                                                     'snrlrgval': i['snrlrgval'],
                                                     'snrmeddev': i['snrmeddev'],
                                                     'snrmedval': i['snrmedval'],
                                                     'snrsmldev': i['snrsmldev'],
                                                     'snrsmlval': i['snrsmlval'],
                                                     'xrdcpval': i['xrdcpval']
                                                     })
#                         print "found: %s %s" % (k1, k2)
#                     if not k1 in sites_matrix.keys():
#                         print "not found: %s" % k1
#        print len(pandasites_matrix)
        return pandasites_matrix
    
    def updateSitesMetricsData(self, data, now):
        '''Insert/update metrics'''
        
        if safety is 'on': utils.setTestDB()
        if setINTR:
            utils.setTestDB()
            print 'Using INTR Database'
        utils.initDB()
        print "Init DB"
        for key in data.keys():
            i = data[key]
            
            sql = "MERGE INTO sites_matrix_data s_mat USING "
            sql += "(SELECT '%s' AS source, '%s' AS dest, TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss') AS meas_date, %s AS sonarsmlval, %s AS sonarsmldev, %s AS sonarmedval, %s AS sonarmeddev, %s AS sonarlrgval, %s AS sonarlrgdev, %s AS perfsonaravgval, %s AS xrdcpval FROM DUAL) m_vals " % (i['src'], i['dst'], now, i['snrsmlval'], i['snrsmldev'], i['snrmedval'], i['snrmeddev'], i['snrlrgval'], i['snrlrgdev'], i['psnravgval'], i['xrdcpval'])
            sql += "ON "
            sql += "(s_mat.source=m_vals.source AND s_mat.destination=m_vals.dest) "
            sql += "WHEN NOT MATCHED THEN INSERT (source, destination, meas_date, sonarsmlval, sonarsmldev, sonarmedval, sonarmeddev, sonarlrgval, sonarlrgdev, perfsonaravgval, xrdcpval) "
            sql += "VALUES "
            sql += "(m_vals.source, m_vals.dest, m_vals.meas_date, m_vals.sonarsmlval, m_vals.sonarsmldev, m_vals.sonarmedval, m_vals.sonarmeddev, m_vals.sonarlrgval, m_vals.sonarlrgdev, m_vals.perfsonaravgval, m_vals.xrdcpval) "
            sql += "WHEN MATCHED THEN UPDATE SET meas_date=m_vals.meas_date, sonarsmlval=m_vals.sonarsmlval, sonarsmldev=m_vals.sonarsmldev, sonarmedval=m_vals.sonarmedval, sonarmeddev=m_vals.sonarmeddev, sonarlrgval=m_vals.sonarlrgval, sonarlrgdev=m_vals.sonarlrgdev, perfsonaravgval=m_vals.perfsonaravgval, xrdcpval=m_vals.xrdcpval"
            
            try:
                utils.dictcursor().execute(sql)
            except:
                print "SQL failed: %s" % sql 
        utils.commit()
        return True
    
    def Proceed(self):
        print datetime.now().replace(microsecond=0)
        print 'Download atlassites src-dst matrix from AGIS'
        sites_matrix = self.getData()
        
        print datetime.now().replace(microsecond=0)
        print 'Download pandaresources for atlassites for mapping from AGIS'
        pandaresources = self.getMapping()
        
        print datetime.now().replace(microsecond=0)
        print 'Build pandasites src-dst matrix'
        pandasites_matrix = self.buildPandaQueueMatrix(sites_matrix, pandaresources)
        
        now = datetime.now().replace(microsecond=0)
        
        print datetime.now().replace(microsecond=0)
        print 'Insert/Update data to sites_matrix_data'
        self.updateSitesMetricsData(pandasites_matrix, now)
        
        print datetime.now().replace(microsecond=0)
        print "Done"