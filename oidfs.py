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

    def __getPStor(self, pstorpath):
        ''' Creates a PStructStor to store Oids '''
        if not os.path.isdir(pstorpath):
            os.mkdir(pstorpath)
        return PStructStor(pstorpath)

    def __writeRootoid(self, rootoid):
        ''' writes the root OID to file '''
        # pickle
        rootoidPath = os.path.join(self.__storpath, OidFS.rootoid_filename)
        fobj = open(rootoidPath, "w")
        cPickle.dump(rootoid, fobj, -1)
        fobj.close()

    def __readRootoid(self):
        ''' Reads and return oid root from file '''
        # unpickle
        rootoidPath = os.path.join(self.__storpath, OidFS.rootoid_filename)
        fobj = open(rootoidPath, "r")
        rootoid = cPickle.load(fobj)
        fobj.close()
        return rootoid

    def __init__(self, storpath):
        if not os.path.isabs(storpath):
            raise ValueError("storpath for OidFS must be absolute")
        if not os.path.isdir(storpath):
            os.mkdir(storpath)
        self.__storpath = storpath
        # Get a PStructStor to store our OID Ptrie
        pstorPath = os.path.join(storpath, OidFS.oidtable_pstor_dir)
        self.__oidPstor = self.__getPStor(pstorPath)
        # Use a Ptrie as our oid table
        self.__ptrieObj = Ptrie(self.__oidPstor)
        # Create or find the root of OID trie: The root OID is saved in a file
        rootoidPath = os.path.join(storpath, OidFS.rootoid_filename)
        if not os.path.exists(rootoidPath):
            # Create OID root, use OID.Nulloid as the root of an empty OID table
            self.__rootoid = ptrie.Nulltrie
            self.__writeRootoid(self.__rootoid)
        else:
            # load rootoid from file
            self.__rootoid = self.__readRootoid()

    def save(self, oid, oidname):
        ''' Save an OID as oidname in our database '''
        print "Saving %s into %s" % (oid, self.__rootoid)
        self.__rootoid = self.__ptrieObj.insert(self.__rootoid, oidname, oid)
        # garbage collect
        self.__rootoid, = self.__oidPstor.keepOids([self.__rootoid])
        self.__writeRootoid(self.__rootoid)

    def find(self, oidname):
        ''' Find an OID based on oidname (that was used to save the OID
        originally '''
        oidnode = self.__ptrieObj.find(self.__rootoid, oidname)
        if not oidnode:
            return ptrie.Nulltrie
        fields = self.__ptrieObj.getfields(oidnode)
        return fields['value']

    def delete(self, oidname):
        self.__rootoid = self.ptrieObj.delete(self.__rootoid, oidname)
        # garbage collect
        self.__rootoid, = self.__oidPstor.keepOids([self.__rootoid])
        self.__writeRootoid(self.__rootoid)
