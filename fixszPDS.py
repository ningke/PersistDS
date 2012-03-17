#!/usr/bin/env python

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
        filesz = fobj.tell()
        if filesz == 0:
            self.fobj.truncate(recsize)
            self.fobj.seek(0, 0)

    def close(self):
        self.fobj.close()
        self.fobj = None

    def __locate(self, seqnum):
        ''' seek the offset denoted by @seqnum. Throws an exception if that
        offset is not less than file size '''
        # Get file size
        self.fobj.seek(0, 2)
        filesz = self.fobj.tell()
        offset = seqnum * self.recsize
        if offset >= filesz:
            raise ValueError("Bad seqnum of %d (recsize %d filesize %d)" % (
                    seqnum, self.recsize, filesz))
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
            # Extend file to off1
            self.fobj.truncate(off1 + self.recsize)
        seqnum = (off1 / self.recsize)
        #print "Spool%d: created oid @ offset %x seqnum %d" % (self.recsize, off1, seqnum)
        return OID(seqnum, self.recsize)

    def retrieve(self, seqnum):
        ''' Returns the record at @seqnum '''
        self.__locate(seqnum)
        #print "Spool%d: retrieving rec @ seqnum %d" % (self.recsize, seqnum)
        return self.fobj.read(self.recsize)

    def update(self, seqnum, offset, partial):
        ''' Change a record partially at offset with new value partial '''
        if len(partial) > self.recsize:
            raise ValueError("newValue too large")
        self.__locate(seqnum)
        # seek to offset within the record and overwrite
        self.fobj.seek(offset, 1)
        mark = self.fobj.tell()
        self.fobj.write(partial)
        self.fobj.seek(mark, 0)
        return self.fobj.read(self.recsize)


def roundToPowerOf2(sz):
    ''' Rounds to the smallest power of 2 that is not less than @sz. Ex.:
    2 => 2, 50 => 64'''
    if sz == 0:
        return 0
    r = 1
    num = sz
    while num:
        num >>= 1
        r *= 2
    if r == 2 * sz:
        r = sz
    return r

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
        self.__stordir= stordir
        # Global StorPool dict
        self.__stor_pools = {}
        for fname in os.listdir(self.__stordir):
            m = FixszPDS.namepat.match(fname)
            if m is None:
                continue
            recsize = int(m.group(1))
            fpath = os.path.join(self.__stordir, fname)
            fo = open(fpath, "rb+")
            self.__stor_pools[fname] = StorPool(recsize, fo)

    @property
    def stordir(self):
        return self.__stordir

    def close(self):
        ''' Call this method when done'''
        for fname, spool in self.__stor_pools.items():
            print "Closing %s" % fname
            spool.fobj.close()

    def expunge(self):
        ''' Delete or otherwise Invalidate all records in the storage and
        reclaim storage space '''
        self.close()
        self.__stor_pools = {}
        for fname in os.listdir(self.__stordir):
            os.remove(os.path.join(self.__stordir, fname))
 
    def __str__(self):
        return "Fixed-Size Storage at %s" % self.__stordir

    def __getStorPool(self, recsize):
        ''' Returns a stor pool of @recsize, the stor pool is created if one
        doesn't exists '''
        recsize = roundToPowerOf2(recsize)
        if recsize == 0:
            raise ValueError("There is no zero sized storage pool.")
        fname = FixszPDS.nameOfStorfile(recsize)
        if fname in self.__stor_pools:
            return self.__stor_pools[fname]
        # Create a new stor pool and add it to the dict
        fpath = os.path.join(self.__stordir, fname)
        assert(not os.path.exists(fpath))
        fo = open(fpath, "wb+")
        spool = StorPool(recsize, fo)
        self.__stor_pools[fname] = spool
        return spool

    def create(self, rec):
        sz = len(rec)
        if sz == 0:
            return OID.Nulloid
        spool = self.__getStorPool(len(rec))
        return spool.create(rec)

    def getrec(self, oid):
        if type(oid) is not OID:
            raise TypeError("oid Must be type OID")
        if oid is OID.Nulloid:
            return ""
        spool = self.__getStorPool(oid.size)
        return spool.retrieve(oid.oid)

    def updaterec(self, oid, offset, newValue):
        if type(oid) is not OID:
            raise TypeError("oid Must be type OID")
        if oid is OID.Nulloid:
            return ""
        spool = self.__getStorPool(oid.size)
        if len(newValue) == 0:
            return spool.retrieve(oid.oid)
        return spool.update(oid.oid, offset, newValue)
