##########################################################################################
# Tools for handling installed software operations in newController                      #
#                                                                                        #
# Alden Stradling 21 Oct 2010                                                            #
# Alden.Stradling@cern.ch                                                                #
##########################################################################################

from SchedulerUtils import utils

from miscUtils import *
from controllerSettings import *
from dbAccess import *


def translateTags(d):
	'''Translate any legacy BDII tags and return new, clean dictionaries. Assumes a dictionary of lists'''
	for key in d:
		for t in tagsTranslation:
			d[key] = [tag.replace(t,tagsTranslation[t]) for tag in d[key]]
	

def updateInstalledSW(lcgdict):
	sw = loadInstalledSW()
	tags = lcgdict.CEtags
	ctags = lcgdict.CEctags

	
	
