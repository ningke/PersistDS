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


from oid import OID

# Fixed size records
class StorPool(object):
    ''' Uses a sequence number for the obj_id '''
    def __init__(self, recsize, fobj):
        self.recsize = recsize
        self.fobj = fobj
        # If the file is newly created (file size is 0), leave one unused
        # recsize in the beginning because oid of 0 is not allowed.
        self.fobj.seek(0, 2)
        self.filesz = fobj.tell()
        if self.filesz == 0:
            self.fobj.truncate(recsize)
            self.fobj.seek(0, 0)
            self.filesz = recsize

    def close(self):
        self.fobj.close()
        self.fobj = None

    def _locate(self, seqnum):
        ''' seek the offset denoted by @seqnum. Throws an exception if that
        offset is not less than file size '''
        # Get file size
        offset = seqnum * self.recsize
        if offset >= self.filesz:
            self.fobj.seek(0, 2)
            print "Real file size %d" % self.fobj.tell()
            raise ValueError("Bad seqnum of %d (recsize %d filesize %d)" % (
                    seqnum, self.recsize, self.filesz))
        self.fobj.seek(offset, 0)

    def create(self, rec):
        ''' Creates a record @rec and returns the oid '''
        if len(rec) > self.recsize:
            raise ValueError("Record too big")
        # Append the record the end of file
        self.fobj.seek(0, 2)
        off1 = self.fobj.tell()
        self.fobj.write(rec)
        off2 = self.fobj.tell()
        if (off2 < off1 + self.recsize):
            # Extend file to (off1 + self.recsize)
            self.fobj.truncate(off1 + self.recsize)
        self.filesz += self.recsize
        seqnum = (off1 / self.recsize)
        #print "Spool%d: created oid @ offset %x seqnum %d" % (self.recsize, off1, seqnum)
        return OID(seqnum, self.recsize)

    def retrieve(self, seqnum):
        ''' Returns the record at @seqnum '''
        self._locate(seqnum)
        #print "Spool%d: retrieving rec @ seqnum %d" % (self.recsize, seqnum)
        return self.fobj.read(self.recsize)

    def update(self, seqnum, offset, partial):
        ''' Change a record partially at offset with new value partial '''
        if len(partial) > self.recsize:
            raise ValueError("newValue too large")
        self._locate(seqnum)
        # seek to offset within the record and overwrite
        self.fobj.seek(offset, 1)
        mark = self.fobj.tell()
        self.fobj.write(partial)
        self.fobj.seek(mark, 0)
        return self.fobj.read(self.recsize)


def roundToPowerOf2(sz):
    ''' Rounds to the smallest power of 2 that is not less than @sz. Ex.:
    2 => 2, 50 => 64'''
    if (sz & (sz - 1) == 0):
        return sz
    p = 0
    while sz:
        sz >>= 1
        p += 1
    return (1 << p)

import os
import re
class FixszPDS(object):
    ''' Fixed size storage for persistent data structures '''
    # Stor Pool file name
    namepat = re.compile('^size_(\d+)')

    @staticmethod
    def nameOfStorfile(recsize):
        return "size_%d" % recsize

    def __init__(self, stordir):
        ''' Initializes storage given a directory, the directory can be 
        empty, in which case a new storage is created, or it can be non-empty
        , in which case existing stor pools are initialized from the storpool
        files (size_32, size_128, etc)'''
        # directory containing all storpool files
        self._stordir= stordir
        # Global StorPool dict
        self._stor_pools = {}
        for fname in os.listdir(self._stordir):
            m = FixszPDS.namepat.match(fname)
            if m is None:
                continue
            recsize = int(m.group(1))
            fpath = os.path.join(self._stordir, fname)
            fo = open(fpath, "rb+")
            self._stor_pools[fname] = StorPool(recsize, fo)

    @property
    def stordir(self):
        return self._stordir

    def close(self):
        ''' Call this method when done'''
        for fname, spool in self._stor_pools.items():
            #print "Closing %s" % fname
            spool.close()

    def expunge(self):
        ''' Delete or otherwise Invalidate all records in the storage and
        reclaim storage space '''
        self.close()
        self._stor_pools = {}
        for fname in os.listdir(self._stordir):
            os.remove(os.path.join(self._stordir, fname))
 
    def __str__(self):
        return "Fixed-Size Storage at %s" % self._stordir

    def _getStorPool(self, recsize):
        ''' Returns a stor pool of @recsize, the stor pool is created if one
        doesn't exists '''
        if recsize == 0:
            raise ValueError("There is no zero sized storage pool.")
        recsize = roundToPowerOf2(recsize)
        fname = FixszPDS.nameOfStorfile(recsize)
        if fname in self._stor_pools:
            return self._stor_pools[fname]
        # Create a new stor pool and add it to the dict
        fpath = os.path.join(self._stordir, fname)
        assert(not os.path.exists(fpath))
        fo = open(fpath, "wb+")
        spool = StorPool(recsize, fo)
        self._stor_pools[fname] = spool
        return spool

    def create(self, rec):
        sz = len(rec)
        if sz == 0:
            return OID.Nulloid
        spool = self._getStorPool(len(rec))
        return spool.create(rec)

    def getrec(self, oid):
        if type(oid) is not OID:
            raise TypeError("oid Must be type OID (Got %s instead)" % type(oid))
        if oid is OID.Nulloid:
            return ""
        spool = self._getStorPool(oid.size)
        return spool.retrieve(oid.oid)

    def updaterec(self, oid, offset, newValue):
        if type(oid) is not OID:
            raise TypeError("oid Must be type OID")
        if oid is OID.Nulloid:
            return ""
        spool = self._getStorPool(oid.size)
        if len(newValue) == 0:
            return spool.retrieve(oid.oid)
        return spool.update(oid.oid, offset, newValue)
