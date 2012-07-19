import os
import struct
import cPickle
import sys

import pstructstor
import oid
import ptrie

# Use PDSCache for internal Ptrie. Comment out the import line below if not
# using pdscache.
import pdscache


##
# TODO
# Pstructstor's garbage collector does not look at oidfs for reference to
# OIDs. This means that if you a PStructStor.keepOids() you must follow with
# a OidFS.save() call. Otherwise any previously stored oids will be stale,
# because those OIDs moved as a result of the garbage collection.
#
class OidFS(object):
    ''' Stores saved Oids within a file system.
    Oids are saved using a Trie data structure (Ptrie) '''
    
    rootoid_filename = "root-oid"
    oidtable_pstor_dir = "pds-storage"

    def _getPStor(self, pstorpath):
        ''' Creates a PStructStor to store Oids '''
        if not os.path.isdir(pstorpath):
            os.mkdir(pstorpath)
        return pstructstor.PStructStor.mkpstor(pstorpath)

    def _writeRootoid(self, rootoid):
        ''' writes the root OID to file '''
        # If PDSCache in use then write through the coid first
        if "pdscache" in sys.modules:
            rootoid = pdscache.write_coid(rootoid)
        rootoidPath = os.path.join(self._storpath, OidFS.rootoid_filename)
        fobj = open(rootoidPath, "w")
        cPickle.dump(rootoid, fobj, 2)
        fobj.close()

    def _readRootoid(self):
        ''' Reads and return oid root from file '''
        # unpickle
        rootoidPath = os.path.join(self._storpath, OidFS.rootoid_filename)
        fobj = open(rootoidPath, "r")
        rootoid = cPickle.load(fobj)
        fobj.close()
        # If PDSCache in use then convert to coid
        if "pdscache" in sys.modules and rootoid is not oid.OID.Nulloid:
            rootoid = pdscache.read_oid(rootoid)
        return rootoid

    def __init__(self, storpath):
        if not os.path.isabs(storpath):
            raise ValueError("storpath for OidFS must be absolute")
        if not os.path.isdir(storpath):
            os.mkdir(storpath)
        self._storpath = storpath
        # Get a PStructStor to store our OID Ptrie
        pstorPath = os.path.join(storpath, OidFS.oidtable_pstor_dir)
        self._oidPstor = self._getPStor(pstorPath)
        # Use a Ptrie as our oid table
        self._ptrieObj = ptrie.Ptrie(self._oidPstor)
        # Create or find the root of OID trie: The root OID is saved in a file
        rootoidPath = os.path.join(storpath, OidFS.rootoid_filename)
        if not os.path.exists(rootoidPath):
            # Create OID root, use OID.Nulloid as the root of an empty OID table
            self._rootoid = ptrie.Nulltrie
            self._writeRootoid(self._rootoid)
        else:
            # load rootoid from file
            self._rootoid = self._readRootoid()

    def close(self):
        self.gc()
        self._oidPstor.close()

    def store(self, oid, oidname):
        ''' Store an OID as oidname in our database '''
        #print "Saving %s into %s" % (oid, self._rootoid)
        # If using pdscache then oid is actually a coid.
        if "pdscache" in sys.modules:
            oid = pdscache.write_coid(oid)
        self._rootoid = self._ptrieObj.insert(self._rootoid, oidname, oid)
        self._writeRootoid(self._rootoid)

    def load(self, oidname):
        ''' Load the OID with ''oidname'' (that was used to save the OID
        originally) from our database. '''
        oidnode = self._ptrieObj.find(self._rootoid, oidname)
        if not oidnode:
            return oidnode
        fields = self._ptrieObj.getfields(oidnode)
        coid = fields['value']
        return coid

    def delete(self, oidname):
        self._rootoid = self._ptrieObj.delete(self._rootoid, oidname)
        self._writeRootoid(self._rootoid)

    def gc(self):
        ''' Does a garbage collection. Saving rootoid only. '''
        o = self._rootoid
        if "pdscache" in sys.modules:
            o = pdscache.write_coid(self._rootoid)
        o, = self._oidPstor.keepOids([o])
        if "pdscache" in sys.modules:
            o = pdscache.read_oid(o)
        self._rootoid = o
        self._writeRootoid(self._rootoid)

    def oriter(self):
        ''' Oid record 'orec' iterator - traverses OidFS In depth-first
        (alphabetical) order. An Oid record is an (oidname, oid) tuple.'''
        for node in self._ptrieObj.dfiter(self._rootoid):
            f = self._ptrieObj.getfields(node)
            if f['final']:
                yield (f['prefix'], f['value'],)
