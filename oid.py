#!/usr/bin/env python

class OID(object):
    Nulloid = None

    def __new__(cls, oid, size):
        if size == 0:
            if OID.Nulloid is None:
                print "Creating Nulloid"
                OID.Nulloid = super(OID, cls).__new__(cls)
                OID.Nulloid.__oid = 0
                OID.Nulloid.__size = 0
                OID.Nulloid.__pstor = None
                OID.Nulloid.__name = "Nulloid"
            return OID.Nulloid
        else:
            return super(OID, cls).__new__(cls)

    def __init__(self, oid, size):
        if self is OID.Nulloid:
            return
        if oid == 0:
            raise RuntimeError("0 is an invalid oid value!")
        self.__oid = oid
        self.__size = size
        self.__pstor = None
        self.__name = "anonymous"

    def __getnewargs__(self):
        return (self.__oid, self.__size)

    @property
    def oid(self):
        return self.__oid
    @property
    def size(self):
        return self.__size
    @property
    def name(self):
        return self.__name
    @name.setter
    def name(self, oname):
        if self is OID.Nulloid:
            raise RuntimeError("Nulloid is read-only!")
        self.__name = oname
    @property
    def pstor(self):
        return self.__pstor
    @pstor.setter
    def pstor(self, pstor):
        if self is OID.Nulloid:
            raise RuntimeError("Nulloid is read-only!")
        self.__pstor = pstor

    def __str__(self):
        if self is OID.Nulloid:
            return "<OID.Nulloid>"
        else:
            return "<OID '%s' %x (size %d)>" % (self.__name,
                    self.__oid, self.__size)

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

