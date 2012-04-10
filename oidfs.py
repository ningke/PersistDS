#!/usr/bin/env python

import os
import struct
from pstructstor import PStructStor
from oid import OID
import ptrie
from ptrie import Ptrie
import cPickle

class OidFS(object):
    ''' Stores saved Oids within a file system.
    Oids are saved using a Trie data structure (Ptrie) '''
    
    rootoid_filename = "root-oid"
    oidtable_pstor_dir = "pds-storage"

    def _getPStor(self, pstorpath):
        ''' Creates a PStructStor to store Oids '''
        if not os.path.isdir(pstorpath):
            os.mkdir(pstorpath)
        return PStructStor(pstorpath)

    def _writeRootoid(self, rootoid):
        ''' writes the root OID to file '''
        # pickle
        rootoidPath = os.path.join(self._storpath, OidFS.rootoid_filename)
        fobj = open(rootoidPath, "w")
        cPickle.dump(rootoid, fobj, -1)
        fobj.close()

    def _readRootoid(self):
        ''' Reads and return oid root from file '''
        # unpickle
        rootoidPath = os.path.join(self._storpath, OidFS.rootoid_filename)
        fobj = open(rootoidPath, "r")
        rootoid = cPickle.load(fobj)
        fobj.close()
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
        self._ptrieObj = Ptrie(self._oidPstor)
        # Create or find the root of OID trie: The root OID is saved in a file
        rootoidPath = os.path.join(storpath, OidFS.rootoid_filename)
        if not os.path.exists(rootoidPath):
            # Create OID root, use OID.Nulloid as the root of an empty OID table
            self._rootoid = ptrie.Nulltrie
            self._writeRootoid(self._rootoid)
        else:
            # load rootoid from file
            self._rootoid = self._readRootoid()

    def save(self, oid, oidname):
        ''' Save an OID as oidname in our database '''
        print "Saving %s into %s" % (oid, self._rootoid)
        self._rootoid = self._ptrieObj.insert(self._rootoid, oidname, oid)
        self._writeRootoid(self._rootoid)

    def find(self, oidname):
        ''' Find an OID based on oidname (that was used to save the OID
        originally '''
        oidnode = self._ptrieObj.find(self._rootoid, oidname)
        if not oidnode:
            return ptrie.Nulltrie
        fields = self._ptrieObj.getfields(oidnode)
        return fields['value']

    def delete(self, oidname):
        self._rootoid = self._ptrieObj.delete(self._rootoid, oidname)
        self._writeRootoid(self._rootoid)

    def gc(self):
        ''' Does a garbage collection. Saving rootoid only. '''
        self._rootoid, = self._oidPstor.keepOids([self._rootoid])
        self._writeRootoid(self._rootoid)

    def lsoid(self):
        def display(node):
            f = self._ptrieObj.getfields(node)
            if f['final']:
                print "(%s : %s)" % (f['prefix'], f['value'])
            else:
                print "Internal (%s : %s)" % (f['prefix'], f['value'])
        self._ptrieObj.dfSearch(self._rootoid, display)
