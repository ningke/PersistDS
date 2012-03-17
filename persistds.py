#!/usr/bin/env python

import cPickle

class PicklePacker(object):
    ''' Uses Python's Pickle protocol 2 and above to pack/unpack PStructs '''
    def __init__(self):
        # Needs at least protocol 2 for __getnewargs__
        self.ver = cPickle.HIGHEST_PROTOCOL
    def pack(self, o):
        return cPickle.dumps(o, self.ver)
    def unpack(self, strbuf):
        return cPickle.loads(strbuf)


import pstructstor
from oid import OID

class PStruct(object):
    default_packer = PicklePacker()
    psobj_table = {}

    @staticmethod
    def mkpstruct(sname, sspec=None):
        ''' Creates and caches (find || create) a PStruct object. A PStruct
        object of a specifc name is created only once. '''
        if sname in PStruct.psobj_table:
            return PStruct.psobj_table[sname]
        elif sspec is None:
            # sspec is required for first time creation
            raise KeyError("PStruct %s not seen and sspec is None!" % sname)
        psobj = PStruct.__new__(PStruct, sname, sspec)
        psobj.__init__(sname, sspec)
        PStruct.psobj_table[sname] = psobj
        return psobj

    #
    # This is intended to be a "private" constructor: Call the staticmethod
    # PStruct.mkpstruct() to create an instance of PStruct.
    #
    def __init__(self, sname, sspec):
        ''' Creates a PStruct that is actually a factory to make structs of
        type defined by @sname and @sspec.
        @sname - Type name of the struct to be defined, it must be unique
        @sspec - A list or tuple of (field names, default value) tuple. '''
        if type(sname) is not str:
            raise TypeError("sname is not type str")
        self.sname = sname
        if not (type(sspec) is tuple or type(sspec) is list):
            raise TypeError("sspec must be a list of tuple")
        self.sspec = tuple(sspec)
        self.sspec_fields = tuple([f[1] for f in sspec])

    def __fieldIndex(self, fname):
        for i, f in enumerate(self.sspec):
            if f[0] == fname:
                return i
        raise KeyError("%s: Bad field name '%s'" % (self, fname))

    def __list2Dict(self, fields=[]):
        res = {}
        for f, spec in zip(fields, self.sspec):
            res[spec[0]] = f
        return res

    def __dict2List(self, fieldsDict):
       sf = list(self.sspec_fields)
       for k, v in fieldsDict.items():
           sf[self.__fieldIndex(k)] = v
       return sf

    def unpackRec(self, rec):
        return PStruct.default_packer.unpack(rec)

    def packFields(self, fields):
        return PStruct.default_packer.pack(fields)

    def __str__(self):
        return "<PStruct %s>" % self.sname

    def initOid(self, oid):
        ''' initializes a "raw" oid, created by storage. The OID already has
        all the fields, but an OID object needs to some additional run-time
        properties setup, like, its name '''
        oid.name = self.sname

    def __make(self, pstor, fields):
        rec = self.packFields(fields)
        oid = pstor.create(rec)
        self.initOid(oid)
        return oid

    def make(self, pstor, **kwargs):
        ''' pass fields in keyword args '''
        fields = self.__dict2List(kwargs)
        return self.__make(pstor, fields)
    
    def checkType(self, o):
        if o.name != self.sname:
            raise TypeError("Wrong OID type: Expecting %s, got %s"
                    % (self.sname, o.name))

    def getfields(self, pstor, o):
        self.checkType(o)
        rec = pstor.getrec(o)
        fields = self.unpackRec(rec)
        return self.__list2Dict(fields)
