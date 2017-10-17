#!/bin/env python
import time, sys, traceback, os, commands, urllib
from datetime import datetime

class SchedulerUtils:

    updateURL = "http://panda.cern.ch:25980/server/pandamon/query?"
    
    test       = 0
    safe       = 1
    dbname     = 'pmeta'
    
    def setTestDB(self):
        self.test=1
        self.dbname='intr'

    def initDB(self):
        from PandaMonitorUtils import utils as monutils
        monutils.initDB(self.dbname)

    def dictcursor(self):
        return self

    def execute(self,query,valdict={}, unconstrained=False, arraysize=100, localdebug=False, edit=True):
        from PandaMonitorUtils import utils as monutils
        return monutils.dictcursor(self.dbname).execute(query,valdict, unconstrained, arraysize, localdebug, edit)

    def fetchall(self):
        from PandaMonitorUtils import utils as monutils
        return monutils.dictcursor(self.dbname).fetchall()

    def commit(self):
        from PandaMonitorUtils import utils as monutils
        monutils.commit(self.dbname)

    def endDB(self):
        from PandaMonitorUtils import utils as monutils
        monutils.endDB(self.dbname)

    def replaceDB(self, table, dict, action='replace',key=''):
        from PandaMonitorUtils import utils as monutils
        return monutils.replaceDB(self.dbname,table, dict, action, key)

    def reportError(self, str, die=False):
        print "ERROR: %s" % str
        txt = "<font color=red><p><b>%s</b></font>" % str
        type, value, tback = sys.exc_info()
        if not type:
            self.navmain = txt
            return txt
        txt += "<font color=red><br>&nbsp; &nbsp; %s: %s" % ( type, value )
        print "       %s: %s" % ( type, value )
        tblines = traceback.extract_tb(tback)
        for l in tblines:
            fname = l[0][l[0].rfind('/')+1:]
            txt += "<br>&nbsp; &nbsp; %s: %s: %s: %s" % ( fname, l[1], l[2], l[3] )
            print "       %s: %s: %s: %s" % ( fname, l[1], l[2], l[3] )
        txt += "</font><p>"
        self.navmain = txt
        if die: raise type, value, tback
        return txt

    def hasTag(self, tags, tag):
        if not tags: return 0
        if tag in tags.split(): return 1
        return 0

    def addTag(self, queue, tag):
        utils.initDB()
        query = "SELECT nqueues, queues FROM taginfo where tag='%s'" % tag
        utils.dictcursor().execute(query)
        therows = utils.dictcursor().fetchall()
        nrows = len(therows)
        if nrows > 0:
            nq = therows[0]['nqueues']
            queuelist= therows[0]['queues'].split()
            if queue in queuelist:
                return 0
            queuedict = dict([(i,1) for i in  queuelist + [queue]])
            if True:
                nq = len(queuedict)
                queues = ' '.join(queuedict.keys())
                updd = { 'tag' : tag, 'nqueues' : nq, 'queues' : queues }
                updl = ( updd, )
                utils.replaceDB('taginfo', updl)
                query = "SELECT tags FROM schedconfig where nickname='%s'" % queue
                utils.dictcursor().execute(query)
                therows = utils.dictcursor().fetchall()
                nrows = len(therows)
                if nrows > 0:
                    tagdict = dict([(i,1) for i in therows[0]['tags'].split() + [tag]])
                    tags=' '.join(tagdict.keys())
                    updd = { 'nickname' : queue, 'tags' : tags }
                    updl = ( updd, )
                    utils.replaceDB('schedconfig', updl)
        else:
            print "Failed to add tag %s to queue %s" ( tag, queue )
        utils.endDB()

    def removeTag(self, queue, tag):
        utils.initDB()
        query = "SELECT nqueues, queues FROM taginfo where tag='%s'" % tag
        utils.dictcursor().execute(query)
        therows = utils.dictcursor().fetchall()
        nrows = len(therows)
        if nrows > 0:
            nq = therows[0]['nqueues']
            queuedict = dict([(i,1) for i in therows[0]['queues'].split()])
            try:
                queuedict.pop(queue)
            except KeyError:
                return 0
            queues=' '.join(queuedict.keys())
            nq = len(queuedict)
            updd = { 'tag' : tag, 'nqueues' : nq, 'queues' : queues }
            updl = ( updd, )
            utils.replaceDB('taginfo', updl)
            query = "SELECT tags FROM schedconfig where nickname='%s'" % queue
            utils.dictcursor().execute(query)
            therows = utils.dictcursor().fetchall()
            nrows = len(therows)
            if nrows > 0:
                taglist = therows[0]['tags'].split()
                tagdict = dict([(i,1) for i in taglist if i != tag])
                tags = ' '.join(tagdict.keys())
                updd = { 'nickname' : queue, 'tags' : tags }
                updl = ( updd, )
                utils.replaceDB('schedconfig', updl)
        utils.endDB()

    def isValid(self,val):
        if val == None: return False
        if isinstance(val,datetime):
            if val.year > 1900:
                return True
            else:
                return False
        if isinstance(val,str):
            if val.lower() == 'null': return False
            if val.lower() == 'none': return False
        return True

    def isFilled(self,val):
        if not self.isValid(val): return False
        if isinstance(val,str):
            if val != '': return True
        if isinstance(val,int):
            if val > 0: return True
        return False

    def messageDB(self,query,updict=[]):
        queries = []
        if len(updict) > 0:
            for d in updict:
                q = query
                for p in d:
                    q += "&%s=%s" % ( p, urllib.quote_plus(str(d[p])) )
                queries.append(q)
        else:
            queries.append(query)
        for q in queries:
            cmd = "curl --connect-timeout 20 --max-time 180 -sS '%s%s'" % ( self.updateURL, q )
            st, out = commands.getstatusoutput(cmd)
            if st:
                print "curl failed:"
                print cmd
                print out
                time.sleep(5)
                st, out = commands.getstatusoutput(cmd)
                if st:
                    print "curl failed attempt 2:"
                    print out
                    time.sleep(5)
                    st, out = commands.getstatusoutput(cmd)
                    print 'attempt 3 (last attempt):', out
                    if st:
                        print "curl failed last attempt 3:"
                        print out
        return st, out

utils = SchedulerUtils()
