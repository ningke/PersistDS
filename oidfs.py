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

    def _writeRootoid(self):
        ''' writes the root OID to file '''
        rootoid = self._rootoid
        # If PDSCache in use then write through the coid first
        if isinstance(rootoid, pdscache._CachedOid):
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
        if rootoid is not oid.OID.Nulloid:
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
            self._writeRootoid()
        else:
            # load rootoid from file
            self._rootoid = self._readRootoid()

    def close(self):
        #self.gc()
        self._oidPstor.close()

    def _store(self, o, oname):
        ''' Store an OID as oidname in database but don't write rootoid yet. '''
        #print "Saving %s into %s" % (o, self._rootoid)
        # If using pdscache then o is actually a coid.
        if isinstance(o, pdscache._CachedOid):
            o = pdscache.write_coid(o)
        self._rootoid = self._ptrieObj.insert(self._rootoid, oname, o)

    def store(self, o, oname):
        ''' Store an OID as oname in our database '''
        self._store(o, oname)
        self._writeRootoid()

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
        self._writeRootoid()

    def _collect_pstor(self):
        ''' Run GC on OID's pstructstor. OIDs stored in OidFS will be moved
        as a result. Note this function assumes that stored oids can belong
        to different PStors, which is currently allowed. In the future,
        PStor should probably take possession of OidFS so that there is only
        one PStor in a single OidFS. '''
        # Since PStor's GC function moves OIDs, we have to make a new OID
        # ptrie with the same oidname and new OID references.
        pstordict = {}
        for orec in self.oriter():
            oname, o = orec
            if isinstance(o, pdscache._CachedOid):
                # o is type 'coid' and o.pstor is a PStructStor object
                pstor = o.pstor
            else:
                # o is type 'OID' and o.pstor is a string
                pstor = pstructstor.PStructStor.mkpstor(o.pstor)
            if pstor not in pstordict:
                # pstor dictionary's value is a (onames, ovalues) pair
                pstordict[pstor] = [[], []]
            onames, ovalues = pstordict[pstor]
            onames.append(oname)
            if isinstance(o, pdscache._CachedOid):
                # Must convert back to real Oid
                o = pdscache.write_coid(o)
            ovalues.append(o)
        if not len(pstordict):
            return
        # Now send to pstructstor for GC and essentially re-create our
        # internal oid ptrie with new oid values
        for pstor in pstordict:
            onames, ovalues = pstordict[pstor]
            pstordict[pstor][1] = ovalues = pstor.keepOids(ovalues)
            for oname, o in zip(onames, ovalues):
                self._store(o, oname)
        self._writeRootoid()

    def gc(self):
        ''' Garbage collects OidFS's internal Ptrie PStor. Saving only
        self._rootoid. '''
        # Run GC on OID's pstor first.
        self._collect_pstor()
        # Save oidfs's _rootoid
        o = self._rootoid
        if isinstance(self._rootoid, pdscache._CachedOid):
            o = pdscache.write_coid(self._rootoid)
        o, = self._oidPstor.keepOids([o])
        o = pdscache.read_oid(o)
        self._rootoid = o
        self._writeRootoid()

    def oriter(self):
        ''' Oid record 'orec' iterator - traverses OidFS In depth-first
        (alphabetical) order. An Oid record is an (oidname, oid) tuple.'''
        for node in self._ptrieObj.dfiter(self._rootoid):
            f = self._ptrieObj.getfields(node)
            if f['final']:
                yield (f['prefix'], f['value'],)
