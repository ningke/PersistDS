# Copyright 2012 Ning Ke
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import struct
import persistds
from fixszPDS import *
import cPickle
import weakref

class PicklePacker(object):
    ''' Uses Python's Pickle protocol 2 and above to pack/unpack PStructs '''
    def __init__(self):
        # Needs at least protocol 2 for __getnewargs__
        self.ver = cPickle.HIGHEST_PROTOCOL
    def pack(self, o):
        return cPickle.dumps(o, self.ver)
    def unpack(self, strbuf):
        return cPickle.loads(strbuf)


class PStructStor(object):
    ''' Manages a pair of OID stores and has the ability to copy/move OIDs
    between the two. This can be used by a garbage collector to "copy collect"
    dead OIDs. '''
    
    # OID packer
    default_packer = PicklePacker()

    # Active/Standby PDS
    mem1name = "mem1"
    mem2name = "mem2"
    activename = "active"

    # Global PStor Table
    _pstor_table = {}

    @staticmethod
    def mkpstor(stordir):
        ''' Create a new PStor or return an existing PStor when ''stordir''
        exists. Use this function instead of using the constructor directly
        to avoid having multiple PStors pointing to the same underlying
        PDS, as dictated by the ''stordir''. '''
        if not os.path.isabs(stordir):
            raise TypeError("Must pass an absolute path as stordir (%s)"
                            % stordir)
        if stordir in PStructStor._pstor_table:
            pstorObj = PStructStor._pstor_table[stordir]()
            if pstorObj:
                return pstorObj
            else:
                # pstorObj has been garbage collected, need to recreate
                # delete entry to prevent __init__ from asserting
                del PStructStor._pstor_table[stordir]
        # Create new pstorObj
        pstorObj = PStructStor.__new__(PStructStor, stordir)
        pstorObj.__init__(stordir)
        PStructStor._pstor_table[stordir] = weakref.ref(pstorObj)
        return pstorObj

    def _create_pds(self, stor_dir):
        ''' Creates PStor top directory. ''stor_dir'' is an absolute path '''
        if not os.path.isdir(stor_dir):
            os.mkdir(stor_dir)
        m1 = os.path.join(stor_dir, PStructStor.mem1name)
        m2 = os.path.join(stor_dir, PStructStor.mem2name)
        for p in m1, m2:
            if not os.path.isdir(p):
                os.mkdir(p)
        self._stordir = stor_dir
        self._pds1 = FixszPDS(m1)
        self._pds2 = FixszPDS(m2)
        active = os.path.join(self._stordir, PStructStor.activename)
        # Set active to point to mem1 initially
        if not os.path.exists(active):
            os.symlink(m1, active)

    def _set_active(self, which):
        m1 = os.path.join(self._stordir, PStructStor.mem1name)
        m2 = os.path.join(self._stordir, PStructStor.mem2name)
        active = os.path.join(self._stordir, PStructStor.activename)
        os.remove(active)
        if which == "1":
            os.symlink(m1, active)
            self.active_pds = self._pds1
            self.standby_pds = self._pds2
        elif which == "2":
            os.symlink(m2, active)
            self.active_pds = self._pds2
            self.standby_pds = self._pds1
        else:
            assert(False)

    def _get_active(self):
        active = os.path.join(self._stordir, PStructStor.activename)
        dst = os.readlink(active)
        if os.path.basename(dst) == PStructStor.mem1name:
            return "1"
        elif os.path.basename(dst) == PStructStor.mem2name:
            return "2"
        else:
            assert(False)

    def _swap_active(self):
        if self.active_pds == self._pds1:
            self._set_active("2")
        elif self.active_pds == self._pds2:
            self._set_active("1")
        else:
            assert(False)

    def __init__(self, stor_dir):
        ''' Must use PStructStor.mkpstor() to create pstor. '''
        # Kill program if someone tries to construct a pstor object directly.
        assert(stor_dir not in PStructStor._pstor_table)
        self._create_pds(stor_dir)
        # Set active pds according to the active link
        self._set_active(self._get_active())
        self.moving = False
        self.reset_stats()

    def reset_stats(self):
        ''' Reset stats. This should be done before a GC. Two sets of stats are kept.
        One set is based on all the Oids ever created. The other set is based on
        only those Oids that are accessed (through getrec()), and therefore it
        reflects the actual "seek" cost more accurately. '''
        # Total set
        self.tot_oids = 0
        self.avg_chld_distance = 0.0
        self.tot_jumps = 0
        # accessed set.
        self.accessed_tot_oids = 0
        self.accessed_avg_chld_distance = 0.0
        self.accessed_tot_jumps = 0
        # GC stats
        self.garbage_cnt = 0

    def print_stats(self):
        print "Creation Stats for Oids:"
        print "Total Oids %d, Average Child Distance %f, Total Jumps %d." % \
            (self.tot_oids, self.avg_chld_distance, self.tot_jumps)
        print "Access Stats for Oids:"
        print "Total Oids %d, Average Child Distance %f, Total Jumps %d." % \
            (self.accessed_tot_oids, self.accessed_avg_chld_distance, self.accessed_tot_jumps)
        print "Garbage Count %d" % self.garbage_cnt

    def __str__(self):
        return "<PStructStor @ %s>" % (self._stordir)

    # the format for packing oid value using the module struct
    oidvalPackFormat = "<Q"
    
    @staticmethod
    def _packOidval(oidval):
        return struct.pack(PStructStor.oidvalPackFormat, oidval)

    @staticmethod
    def _unpackOidval(rec):
        return struct.unpack(PStructStor.oidvalPackFormat, rec)[0]

    @staticmethod
    def _sizeofPackedOidval():
        return struct.calcsize(PStructStor.oidvalPackFormat)

    def _stampOid(self, oid):
        oid.pstor = self._stordir

    def _checkStamp(self, oid):
        return oid.pstor == self._stordir

    # Interface to pds
    def _create(self, pds, ofields):
        ''' Writes a record in storage and return the OID. A "forward pointer"
        field is added. It points to new "forwarded location during copying.
        The pds to write the record to must be specified '''
        # Pack oid fields (a list)
        oidrec = PStructStor.default_packer.pack(ofields)
        # Newly created OIDs have a zero Oidval as its forward pointer.
        # "Real" OIDs always have a non-zero oid value.
        internalRec = PStructStor._packOidval(0) + oidrec
        o = pds.create(internalRec)
        # Save this pstor inside the OID - Use self._stordir as the unique
        # identification for this pstor
        self._stampOid(o)
        # Collect creation stats
        statstup = (self.tot_oids, self.tot_jumps, self.avg_chld_distance)
        (self.tot_oids, self.tot_jumps, self.avg_chld_distance) = \
            self.cumulate_stats(statstup, o, ofields)
        # Now return the newly created Oid ''o''
        return o

    def cumulate_stats(self, curstats, o, ofields):
        ''' collects stats pertaining to Oid creation. '''
        (tot_oids, tot_jumps, avg_chld_dis) = curstats
        avgdis, jumps = self.calc_children_distance(o, ofields)
        avg_chld_dis = (avg_chld_dis * tot_oids + avgdis) / (tot_oids + 1)
        tot_oids += 1
        tot_jumps += jumps
        return (tot_oids, tot_jumps, avg_chld_dis)

    def calc_children_distance(self, o, ofields):
        ''' find average distance (PDS offset) to its direct children and number of
        "jumps". '''
        (avgdis, jumps) = (0.0, 0)
        cnt = 0
        totaldis = 0
        for f in ofields:
            if isinstance(f, OID) and f is not OID.Nulloid:
                # If this is a "foreign" oid, or if its size differ then it's a
                # jump from one PDS to another, we don't know how far the distance is.
                if self._checkStamp(f) and f.size == o.size:
                    totaldis += abs(o.oid - f.oid)
                    cnt += 1
                else:
                    jumps += 1
        if cnt:
            avgdis = float(totaldis / cnt);
        return (avgdis, jumps)

    def create(self, oidfields):
        ''' Creates an OID object in the active pds '''
        return self._create(self.active_pds, oidfields)

    def _getrec(self, pds, o):
        ''' Get the internal rec for the oid. Unpack and return a tuple of
        (oidval, oidfields) '''
        internalRec = pds.getrec(o)
        offset = PStructStor._sizeofPackedOidval()
        oidvalStr = internalRec[:offset]
        forwardOidval = PStructStor._unpackOidval(oidvalStr)
        rec = internalRec[offset:]
        ofields = PStructStor.default_packer.unpack(rec)
        # Collect access stats
        statstup = (self.accessed_tot_oids, self.accessed_tot_jumps,
                    self.accessed_avg_chld_distance)
        (self.accessed_tot_oids, self.accessed_tot_jumps,
         self.accessed_avg_chld_distance) = self.cumulate_stats(statstup, o, ofields)
        return (forwardOidval, ofields)

    def getrec(self, o):
        unused, oidfields = self._getrec(self.active_pds, o)
        return oidfields

    def close(self):
        self.active_pds.close()
        self.standby_pds.close()

    def keepOids(self, roots):
        ''' Start the moving operation. roots are a list of "root OIDs" to
        save. OIDs will be copied starting from these roots in depth first
        order '''
        if self.moving:
            raise RuntimeError("Cannot run moving operation in parallel")
        oldoidcnt = self.tot_oids
        self.reset_stats()
        self.moving = True
        newroots = []
        for r in roots:
            #print "moving %s" % r
            newroots.append(self._move(r))
        self._swap_active()
        # Expunge the old PDS
        self.standby_pds.expunge()
        self.garbage_cnt = oldoidcnt - self.tot_oids
        self.accessed_tot_oids = 0
        self.accessed_avg_chld_distance = 0.0
        self.accessed_tot_jumps = 0
        self.moving = False
        return newroots

    def _move(self, oid):
        ''' Moves an OID from active to standby '''
        # Don't move OID.Nulloid, it is never stored.
        if oid is OID.Nulloid:
            return OID.Nulloid
        # First get the PStruct that was used to construct the OID
        ps = persistds.PStruct.mkpstruct(oid.name)
        # Read the record referenced by the oid
        forwardOidval, fields = self._getrec(self.active_pds, oid)
        if forwardOidval != 0:
            # this oid is already copied (moved). Just create an OID object
            # that points to the new oidval
            newoid = OID(forwardOidval, oid.size)
            ps.initOid(newoid)
            return newoid
        # Go through each field in the list. If a
        # field is a "regular" Python object or OID.Nulloid, then it
        # remains unchanged, if a field is an OID, then create a new OID at
        # the standby PDS.
        for i, f in enumerate(fields):
            if isinstance(f, OID) and f is not OID.Nulloid:
                # We can only move an OID that is created (and stored) in
                # our own pstor (self). A "foreign" OID is left alone.
                if self._checkStamp(f):
                    fields[i] = self._move(f)
        # Create the new OID object in the standby PDS
        newoid = self._create(self.standby_pds, fields)
        ps.initOid(newoid)
        # Now update the "forward pointer" for the old OID so it won't be
        # moved again
        forwardOidval = PStructStor._packOidval(newoid.oid)
        self.active_pds.updaterec(oid, 0, forwardOidval)
        return newoid
