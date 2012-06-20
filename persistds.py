#!/usr/bin/env python

from oid import OID
import pdscache

class PStruct(object):
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

    def _fieldIndex(self, fname):
        for i, f in enumerate(self.sspec):
            if f[0] == fname:
                return i
        raise KeyError("%s: Bad field name '%s'" % (self, fname))

    def _list2Dict(self, fields=[]):
        res = {}
        for f, spec in zip(fields, self.sspec):
            res[spec[0]] = f
        return res

    def _dict2List(self, fieldsDict):
       sf = list(self.sspec_fields)
       for k, v in fieldsDict.items():
           sf[self._fieldIndex(k)] = v
       return sf

    def __str__(self):
        return "<PStruct %s>" % self.sname

    def initOid(self, oid):
        ''' initializes a "raw" oid, created by storage. The OID already has
        all the fields, but an OID object needs to some additional run-time
        properties setup, like, its name '''
        oid.name = self.sname

    def _make(self, pstor, fields):
        oid = pdscache.oidcreate(fields, pstor)
        self.initOid(oid)
        return oid

    def make(self, pstor, **kwargs):
        ''' pass fields in keyword args '''
        fields = self._dict2List(kwargs)
        return self._make(pstor, fields)
    
    def checkType(self, o):
        if o.name != self.sname:
            raise TypeError("Wrong OID type: Expecting %s, got %s"
                    % (self.sname, o.name))

    def getfields(self, pstor, o):
        self.checkType(o)
        fields = pdscache.oidfields(o)
        return self._list2Dict(fields)
