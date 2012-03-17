#!/usr/bin/env python

import os
import struct
import persistds
from fixszPDS import *

class PStructStor(object):
    ''' Manages a pair of OID stores and has the ability to copy/move OIDs
    between the two. This can be used by a garbage collector to "copy collect"
    dead OIDs. '''

    mem1name = "mem1"
    mem2name = "mem2"
    activename = "active"

    def __create_pds(self, stor_dir):
        ''' stor_dir is an absolute path '''
        if not os.path.isdir(stor_dir):
            os.mkdir(stor_dir)
        m1 = os.path.join(stor_dir, PStructStor.mem1name)
        m2 = os.path.join(stor_dir, PStructStor.mem2name)
        for p in m1, m2:
            if not os.path.isdir(p):
                os.mkdir(p)
        self.__stordir = stor_dir
        self.__pds1 = FixszPDS(m1)
        self.__pds2 = FixszPDS(m2)
        active = os.path.join(self.__stordir, PStructStor.activename)
        # Set active to point to mem1 initially
        if not os.path.exists(active):
            os.symlink(m1, active)

    def __set_active(self, which):
        m1 = os.path.join(self.__stordir, PStructStor.mem1name)
        m2 = os.path.join(self.__stordir, PStructStor.mem2name)
        active = os.path.join(self.__stordir, PStructStor.activename)
        os.remove(active)
        if which == "1":
            os.symlink(m1, active)
            self.active_pds = self.__pds1
            self.standby_pds = self.__pds2
        elif which == "2":
            os.symlink(m2, active)
            self.active_pds = self.__pds2
            self.standby_pds = self.__pds1
        else:
            assert(False)

    def __get_active(self):
        active = os.path.join(self.__stordir, PStructStor.activename)
        dst = os.readlink(active)
        if os.path.basename(dst) == PStructStor.mem1name:
            return "1"
        elif os.path.basename(dst) == PStructStor.mem2name:
            return "2"
        else:
            assert(False)

    def __swap_active(self):
        if self.active_pds == self.__pds1:
            self.__set_active("2")
        elif self.active_pds == self.__pds2:
            self.__set_active("1")
        else:
            assert(False)

    def __init__(self, stor_dir):
        if not os.path.isabs(stor_dir):
            raise TypeError("Must pass an absolute path for stor_dir")
        self.__create_pds(stor_dir)
        # Set active pds according to the active link
        self.__set_active(self.__get_active())
        self.moving = False

    def __str__(self):
        return "<PStructStor @ %s>" % (self.__stordir)

    # the format for packing oid value using the module struct
    oidvalPackFormat = "<Q"
    
    @staticmethod
    def __packOidval(oidval):
        return struct.pack(PStructStor.oidvalPackFormat, oidval)

    @staticmethod
    def __unpackOidval(rec):
        return struct.unpack(PStructStor.oidvalPackFormat, rec)[0]

    @staticmethod
    def __sizeofPackedOidval():
        return struct.calcsize(PStructStor.oidvalPackFormat)

    def __stampOid(self, oid):
        oid.pstor = self.__stordir

    def __checkStamp(self, oid):
        return oid.pstor == self.__stordir

    # Interface to pds
    def __create(self, pds, rec):
        ''' Writes a record in storage and return the OID. A "forward pointer"
        field is added. It points to new "forwarded location during copying.
        The pds to write the record to must be specified '''
        # Newly created OIDs have a zero Oidval as its forward pointer.
        # "Real" OIDs always have a non-zero oid value.
        # The concatenation below is potentially inefficient since the
        # rec string is copied
        internalRec = PStructStor.__packOidval(0) + rec
        oid = pds.create(internalRec)
        # Save this pstor inside the OID - Use self.__stordir as the unique
        # identification for this pstor
        self.__stampOid(oid)
        return oid

    def create(self, rec):
        ''' Creates an OID object in the active pds '''
        return self.__create(self.active_pds, rec)

    def __getrec(self, pds, oid):
        ''' Get the internal rec for the oid. returns a tuple of
        (oidval, rec) '''
        internalRec = pds.getrec(oid)
        offset = PStructStor.__sizeofPackedOidval()
        oidvalStr = internalRec[:offset]
        forwardOidval = PStructStor.__unpackOidval(oidvalStr)
        rec = internalRec[offset:]
        return (forwardOidval, rec,)
    
    def getrec(self, oid):
        unused, rec = self.__getrec(self.active_pds, oid)
        return rec
    
    def close(self):
        self.active_pds.close()
        self.standby_pds.close()

    def keepOids(self, roots):
        ''' Start the moving operation. roots are a list of "root OIDs" to
        save. OIDs will be copied starting from these roots in depth first
        order '''
        if self.moving:
            raise RuntimeError("Cannot run moving operation in parallel")
        self.moving = True
        newroots = []
        for r in roots:
            print "moving %s" % r
            newroots.append(self.__move(r))
        self.__swap_active()
        # Expunge the old PDS
        self.standby_pds.expunge()
        self.moving = False
        return newroots

    def __move(self, oid):
        ''' Moves an OID from active to standby '''
        # Don't move OID.Nulloid, it is never stored.
        if oid is OID.Nulloid:
            return OID.Nulloid
        # First get the PStruct that was used to construct the OID
        ps = persistds.PStruct.mkpstruct(oid.name)
        # Read the record referenced by the oid
        forwardOidval, rec = self.__getrec(self.active_pds, oid)
        if forwardOidval != 0:
            # this oid is already copied (moved). Just create an OID object
            # that points to the new oidval
            newoid = OID(forwardOidval, oid.size)
            ps.initOid(newoid)
            return newoid
        fields = ps.unpackRec(rec)
        # Make a copy of fields, then go through each field in the list. If a
        # field is a "regular" Python object or OID.Nulloid, then it
        # remains unchanged, if a field is an OID, then create a new OID at
        # the standby PDS.
        newfields = fields[:]
        for i, f in enumerate(fields):
            if isinstance(f, OID) and f is not OID.Nulloid:
                # We can only move an OID that is created (and stored) in
                # our own pstor (self). A "foreign" OID is left alone.
                if self.__checkStamp(f):
                    newfields[i] = self.__move(f)
        newrec = ps.packFields(newfields)
        # Create the new OID object in the standby PDS
        newoid = self.__create(self.standby_pds, newrec)
        ps.initOid(newoid)
        # Now update the "forward pointer" for the old OID so it won't be
        # moved again
        forwardOidval = PStructStor.__packOidval(newoid.oid)
        self.active_pds.updaterec(oid, 0, forwardOidval)
        return newoid
