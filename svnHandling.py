#! /usr/bin/env python
#######################################################
# Handles changes to the configuration files and      #
# backup file stored in Subversion as a backup.       #
#                                                     #
# Alden Stradling (Alden.Stradling@cern.ch) 17 Feb 10 #
#######################################################


# Might be nice to add some safeties -- checks on the success of these operations.

import os, sys, commands, time

from controllerSettings import *


def svnCheckout():
    ''' Check out the configuration SVN -- should only be necessary on rare occasions.'''
    return 0
    if svnDebug: print 'Beginning SVN checkout'
    path = os.getcwd()
    try:
        os.chdir(cfg_path)
        os.chdir('..')
    except OSError:
        os.makedirs(cfg_path)
        os.chdir(cfg_path)
        os.chdir('..')
    print '####### Checking out the SVN repository anew -- this should be a RARE event! Is this really what you want to do? #############'
    # Check out the whole repo
    os.system('svn co  %s pandaconf' % confrepo)
    os.chdir(path)
    if svnDebug: print 'Completing SVN checkout'
    return 0


def svnCheckin(notestr=''):
    ''' Update the SVN repo with changes from BDII and ToA. Argument, if provided, notes which files received updates. '''
    if svnDebug: print 'Beginning SVN checkin'
    path = os.getcwd()
    os.chdir(cfg_path)
    # Timestamp in GMT
    message = 'Changes made: %s%s' % (time.asctime(time.gmtime()), notestr)
    # Add all new files before checking in
    for p in [hotBackupPath.split(os.sep)[-1], jdlconfigs.split(os.sep)[-1], configs.split(os.sep)[-1]]:
        o = commands.getoutput('rm -f  %s/msgtmp' % p)
        o = commands.getoutput('svn add %s/*' % p)
    if svnDebug: print o
    o = commands.getoutput('rm -f  %s/*/msgtmp' % p)
    if svnDebug: print o
    o = commands.getoutput('svn add %s/*/*' % p)
    if svnDebug: print o
    o = commands.getoutput('rm -f  %s/*/*/msgtmp' % p)
    if svnDebug: print o
    o = commands.getoutput('svn add %s/*/*/*' % p)
    if svnDebug: print o

    # Check in the subversion
    os.system('svn ci -m "%s"' % (message))
    # Go back to original path
    os.chdir(path)
    if svnDebug: print 'Completing SVN checkin'
    return 0


def svnUpdate():
    ''' Update from the SVN repo -- introduce changes from user configs. '''
    if svnDebug: print 'Beginning SVN update'
    # Get the present path
    path = os.getcwd()
    try:
        # Go to the configs directory path, if possible
        os.chdir(cfg_path)
    except OSError:
        # Or create it if it's not there
        os.makedirs(cfg_path)
        os.chdir(cfg_path)
        os.chdir('..')
    # svnCheckout()
    # Update the whole subversion
    os.system('svn update --accept theirs-full')
    try:
        # Go to the configs directory path, if possible
        os.chdir(cmp_path)
    except OSError:
        # Or create it if it's not there
        os.makedirs(cmp_path)
        os.chdir(cmp_path)
        os.chdir('..')
    # svnCheckout()
    # Update the whole subversion
    os.system('svn update --accept theirs-full')

    # Back to where you were.
    os.chdir(path)
    if svnDebug:
        print 'Completing SVN update'
        return 0
    else:
        os.chdir(path)
        return 1


def svnRemoveFiles(d):
    '''Remove config files due to AGIS deletion'''

    path = os.getcwd()
    os.chdir(configs)
    for i in d:
        c = d[i]['cloud']
        s = d[i]['site']
        q = d[i][dbkey]
        if svnDebug: print 'Removing queue file %s.py' % q
        # Add all new files before checking in
        o = commands.getoutput('svn rm  %s/%s/%s%s' % (c, s, q, postfix))

    # Check in the subversion
    os.system('svn ci -m "AGIS-driven deletion"')
    # Go back to original path
    os.chdir(path)
    if svnDebug: print 'Completing SVN deletions'
    return 0
