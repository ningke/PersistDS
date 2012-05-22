#!/usr/bin/env python

class OID(object):
    Nulloid = None

    def __new__(cls, oid, size):
        if size == 0:
            if OID.Nulloid is None:
                print "Creating Nulloid"
                OID.Nulloid = super(OID, cls).__new__(cls)
                OID.Nulloid._oid = 0
                OID.Nulloid._size = 0
                OID.Nulloid._pstor = None
                OID.Nulloid._name = "Nulloid"
            return OID.Nulloid
        else:
            return super(OID, cls).__new__(cls)

    def __init__(self, oid, size):
        if self is OID.Nulloid:
            return
        if oid == 0:
            raise RuntimeError("0 is an invalid oid value!")
        self._oid = oid
        self._size = size
        self._pstor = None
        self._name = "anonymous"

    def __getnewargs__(self):
        return (self._oid, self._size)

    @property
    def oid(self):
        return self._oid
    @property
    def size(self):
        return self._size
    @property
    def name(self):
        return self._name
    @name.setter
    def name(self, oname):
        if self is OID.Nulloid:
            raise RuntimeError("Nulloid is read-only!")
        self._name = oname
    @property
    def pstor(self):
        return self._pstor
    @pstor.setter
    def pstor(self, pstor):
        if self is OID.Nulloid:
            raise RuntimeError("Nulloid is read-only!")
        self._pstor = pstor

    def __str__(self):
        if self is OID.Nulloid:
            return "<OID.Nulloid>"
        else:
            return "<OID '%s' %x (size %d)>" % (self._name,
                    self._oid, self._size)

    def __nonzero__(self):
        return self is not OID.Nulloid

# Create OID.Nulloid
OID(0, 0)

if __name__ == "__main__":
    import pickle

    r0 = OID(0, 0)
    print bool(r0)
    print r0 is OID.Nulloid  # should print True
    r2 = OID(0, 32)
    r3 = OID(0, 32)
    print r2 is r3              # should print False
    print bool(r2)

    # Must use pickle protocol 2: __getnewargs__ only available in 2.
    r0_saved = pickle.dumps(r0, 2)
    r0_recovered = pickle.loads(r0_saved)
    print r0_recovered is OID.Nulloid        # should print True
    print bool(r0_recovered)

