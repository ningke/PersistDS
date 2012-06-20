import weakref
from lnklist import *
import oid
import persistds
import pstructstor

class _CachedOid(object):
    ''' Acts as a "proxy" for an OID. When a PDS instance is first created
    in cache, it's fields are saved in this proxy. At that time, it acts
    as an OID to the running program only. When later the OID is created
    in persistent storage (PStor), then it becomes a proxy with a "backup".
    '''
    # A strictly incrementing counter used to identify an OID. Also used
    # as the hash key.
    _seqcount = 0

    def __init__(self, pstor, o=None):
        ''' A "freshly" created _CachedOid doesn't have a "real" OID ''o'',
        it will be created when the coid is "flushed" to it pstor. On the
        other hand, a _CachedOid created from "backing" OID just need to save
        the ''o''. '''
        _CachedOid._seqcount += 1
        self.seqnum = _CachedOid._seqcount
        assert(isinstance(pstor, pstructstor.PStructStor))
        if o is not None:
            assert(isinstance(o, oid.OID))
        self.pstor = pstor
        self.oid = o
        print "Created coid %d" % self.seqnum

    def add_destructor(self, destructor):
        self._destructor = destructor

    def __del__(self):
        # delete cache entry
        if hasattr(self, "_destructor"):
            self._destructor(self)


# The "centry" functions are helpers to manipulate the cache entries.
class _CacheEntry(object):
    __slots__ = ["seqnum", "oidfields", "coidwref", "lnode"]

    def __init__(self, coid, oidfields):
        assert(isinstance(coid, _CachedOid))
        self.seqnum = coid.seqnum
        self.oidfields = oidfields
        # Careful: circular reference here - (Don't define __del__)
        self.coidwref = weakref.ref(coid)
        self.lnode = ListEntry(self)
        print "Cached coid %d <%s>" % (coid.seqnum, oidfields)


class PDSCache(object):
    ''' Implements a cache for PDS. A PDS oid is always created in cache
    first. Access to an oid goes through the cache also. Cached oids are
    flushed to PStor when cache is getting full. '''

    def __init__(self, max_entries):
        ''' A PDS cache of ''max_entries'' cache slots '''
        self._max_entries = max_entries
        self._num_entries = 0
        # Cache is implemented as a dictionary
        self._cache = {}
        # Least recently used cache entries
        self._lrulist = ListEntry(None)
        # For debug/profile
        self._num_dead_pdsobjs = 0

    def _get_destructor_func(self):
        def delcoidcb(coid):
            ''' Deletes a cache entry. This is passed to a _CachedOid object as
            a destructor callback. '''
            print "Garbage coid %d dead" % coid.seqnum,
            try:
                centry = self._cache[coid.seqnum]
                self._delcentry(centry)
                print " - remove cache entry",
            except KeyError:
                # the coid is not in cache (and must be in PStor). Do nothing.
                pass
            print ""
        return delcoidcb

    def _add(self, coid, oidfields):
        ''' Add a PDS instance to cache. Use the coid's seqnum as the
        dictionary key. '''
        # Check if cache is full, if so, flush some entries to PStor.
        if self._num_entries >= self._max_entries:
            self._flushcache()
        coid.add_destructor(self._get_destructor_func())
        centry = _CacheEntry(coid, oidfields)
        self._addcentry(centry)
        return centry

    def _addcentry(self, centry):
        self._num_entries += 1
        self._cache[centry.seqnum] = centry
        # Most recent entries are added to the tail
        list_add_tail(centry.lnode, self._lrulist)

    def _delcentry(self, centry):
        # Delete from list
        list_del(centry.lnode)
        # This breaks the reference cycle: (''data'' refers to centry)
        # Is this needed? If it doesn't help performance then get rid of it.
        centry.lnode.data = None
        del self._cache[centry.seqnum]
        self._num_entries -= 1

    def _num_to_flush(self):
        ''' Try to determine an optimal number of entries to flush when
        cache is full. '''
        num = int(self._max_entries * 0.05)
        if num < 1:
            num = 1
        if num > self._num_entries:
            num = self._num_entries
        return num

    def _flushcache(self):
        ''' Free up some cache entries by "flusing" the least recently used
        entries to PStor '''
        #num_to_flush = self._num_to_flush()
        num_to_flush = 1
        num_before_flush = self._num_entries
        while True:
            num_flushed = num_before_flush - self._num_entries
            if num_flushed >= num_to_flush:
                break
            # least recently used entries are from head of list
            if list_empty(self._lrulist):
                assert(False)
                break
            node = self._lrulist._next
            centry = node.data
            self._flush(centry)

    def _flush(self, centry):
        ''' Flushes a cache entry ''centry'' to PStor and delete the entry. '''
        coid = centry.coidwref()
        if not coid:
            # the coid is garbage. Just delete it.
            print "Flush: Seeing garbage coid (seqnum %d)" % centry.seqnum
            self._delcentry(centry)
            return
        # First write-through the coid, then delete cache entry
        self._write_coid(coid)
        self._delcentry(centry)
        print "Flushed centry %d" % centry.seqnum

    def _write_coid(self, coid):
        ''' Write the cached oid ''coid'' to PStor. Return the resulting OID.
        If a field in the coid refers to another coid, that coid will be
        written first. As a result, a tree of oids are written through by
        this function. '''
        if coid.oid is not None:
            # This coid has already been written (to PStor). It won't ever
            # change.
            return coid.oid
        # Now this coid MUST be in cache, otherwise it would be a "phantom"
        # coid...
        assert(coid.seqnum in self._cache)
        centry = self._cache[coid.seqnum]
        # Must make a copy of oidfields before doing this since we must not
        # change the oidfields of the coid as a result of this function.
        oidfields = centry.oidfields[:]
        print "Writing coid %d: %s" % (coid.seqnum, oidfields)
        for i, f in enumerate(oidfields):
            if isinstance(f, _CachedOid):
                # Note that we substitute a coid with a "real" OID
                oidfields[i] = self._write_coid(f)
        print "Creating oid: %s" % oidfields
        o = coid.pstor.create(oidfields)
        assert(hasattr(coid, "name"))
        pds = persistds.PStruct.mkpstruct(coid.name)
        pds.initOid(o)
        coid.oid = o
        return coid.oid

    def create(self, oidfields, pstor):
        ''' Interface to PersistDS's OID create '''
        coid = _CachedOid(pstor)
        self._add(coid, oidfields)
        return coid

    def _get_oidrec(self, o):
        ''' Interface to PersistDS's OID getrec when the passed oid is a
        "real" OID ''o''. '''
        # oid.pstor is a string
        pstor = pstructstor.PStructStor(o.pstor)
        oidfields = pstor.getrec(o)
        # Create a _CachedOid based on the real OID
        coid = _CachedOid(pstor, o)
        # Put it back into cache. Note is possible that multiple coids refer
        # to the same underlying OID as a result of doing this. Sigh...
        self._add(coid, oidfields)
        return oidfields

    def _get_coidrec(self, coid):
        ''' Interface to PersistDS's OID getrec when the passed oid is a
        cached oid ''coid''. '''
        try:
            centry = self._cache[coid.seqnum]
            # Coid in cache: Move it to the tail of LRU list
            list_move_tail(centry.lnode, self._lrulist)
            print "Getting centry %d" % centry.seqnum
            return centry.oidfields
        except KeyError:
            # This Oid Cache has been moved to pstor, we have to get it back
            # first
            print "Getting coid (%d) from PStor" % coid.seqnum
            assert(coid.oid is not None)
            oidfields = coid.pstor.getrec(coid.oid)
            # Put it back into cache. Note coid.seqnum is reused here.
            self._add(coid, oidfields)
            return oidfields


##
# All right, this is the global singleton PDS cache object that everyone
# uses.
#
_pdscache = PDSCache(4)

# Use these public functions to create and get OIDs
def oidcreate(oidfields, pstor):
    ''' Create a cached OID. '''
    return _pdscache.create(oidfields, pstor)

def oidfields(o):
    ''' ''o'' can be a either a cached OID or a "real" OID. '''
    if isinstance(o, _CachedOid):
        return _pdscache._get_coidrec(o)
    elif isinstance(o, oid.OID):
        return _pdscache._get_oidrec(o)
    else:
        raise TypeError("Must be OID or _CachedOid type")
