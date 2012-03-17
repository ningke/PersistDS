#!/usr/bin/env python

import time
import os
from fixszPDS import *
from persistds import PStruct

def openPDS():
    testpds_path = "./testpds/fixszPDS001"
    if not os.path.isdir(testpds_path):
        os.mkdir(testpds_path)
    print "Opened PDS at %s" % testpds_path
    return FixszPDS(testpds_path)


some_past_time = 1325851621

##
# A 'plain' record is just a string
#
def addTimeRec(pds):
    now = int(time.time()) - some_past_time
    r1 = str(now)
    print "Adding plain record: '%s'" % r1
    oid = pds.create(r1)
    return oid

def prTimeRec(pds, oid):
    r = pds.getrec(oid)
    r = r.split('\x00', 1)[0]
    print "getrec: oid %s => %s (%d)" % (oid, r, len(r))


##
# Use a PStruct as a "structured" record - which is a dict of fields
#
def mkTimeStruct(pds):
    return PStruct(
            'epoch', # Unix epoch
            {'timestamp' : 0}, # The actual struct
            pds);

def addOne(tps):
    now = int(time.time()) - some_past_time
    oid = tps.make({'timestamp' : now})
    print "Added '%s' as %s" % (now, oid)
    return oid

def getOne(tps, oid):
    f = tps.getfields(oid)
    print "getfields: %s => " % oid, f


# Tests
pds = openPDS()
# Plain records
oid = addTimeRec(pds)
prTimeRec(pds, oid)

# PStruct records
tstruct = mkTimeStruct(pds)
oid = addOne(tstruct)
getOne(tstruct, oid)

# Close PDS
pds.close()

