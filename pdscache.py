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
        #print "Created coid %d" % self.seqnum
        if _cprof:
            _cprof.coidcnt += 1


# The "centry" functions are helpers to manipulate the cache entries.
class _CacheEntry(object):
    __slots__ = ["seqnum", "ofields", "coidwref", "lnode"]

    def __init__(self, coid, ofields):
        assert(isinstance(coid, _CachedOid))
        self.seqnum = coid.seqnum
        self.ofields = ofields
        # Careful: circular reference here - (Don't define __del__)
        self.coidwref = weakref.ref(coid)
        self.lnode = ListEntry(self)
        #print "Cached coid %d <%s>" % (coid.seqnum, ofields)
        if _cprof:
            _cprof.centrycnt += 1


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
        # Number of entries swept (garbage) during last sweeping
        self._last_swept = None
        self._full_since_last_swept = 0

    def _add(self, coid, ofields):
        ''' Add a PDS instance to cache. Use the coid's seqnum as the
        dictionary key. '''
        # Check if cache is full, if so, flush some entries to PStor.
        if self._num_entries >= self._max_entries:
            self._freeup_centries()
            assert(self._num_entries < self._max_entries)
        centry = _CacheEntry(coid, ofields)
        self._addcentry(centry)
        return centry

    def _addcentry(self, centry):
        assert(self._num_entries < self._max_entries)
        self._num_entries += 1
        self._cache[centry.seqnum] = centry
        # Most recent entries are added to the tail
        list_add_tail(centry.lnode, self._lrulist)

    def _delcentry(self, centry):
        #print "delcentry: %d (%s)" % (centry.seqnum, centry.ofields[0])
        # Delete from list
        list_del(centry.lnode)
        # This breaks the reference cycle: (''data'' refers to centry)
        # Is this needed? If it doesn't help performance then get rid of it.
        centry.lnode.data = None
        del self._cache[centry.seqnum]
        self._num_entries -= 1

    def _freeup_centries(self):
        ''' Try to free up some cache entries: First collect all the garbages.
        If no garbages are found then flush out the least recently used cache
        entry. '''
        # Sweeping in this manner is expensive, only do it if garbage amount
        # exceeds a threshold.
        gc_threshold = 0.4
        r1 = float(self._full_since_last_swept) / self._num_entries
        #print self._last_swept, r1
        if self._last_swept is None or r1 > gc_threshold:
            self._sweep_garbage()
            self._full_since_last_swept = 0
            if self._last_swept > 0:
                return
        # Now either there is no garbage or we skipped sweeping
        self._full_since_last_swept += 1
        self._flush_lru_entry()

    def _sweep_garbage(self):
        ''' Go through all the cache entries and sweeps (delete) any cache
        entries that are "garbage", as indicated by a "dead" coid weak ref.
        Since a coid, with its associated cache entry (''centry''), can
        refers to other coids/centries, those referenced coids will be
        dereferced when this coid/centry is deleted, thus producing more
        garbage, which is then collect in a subsequent round. Collection
        is done when zero garbage are collected in a round. '''
        total_swept = 0
        #print "Sweeping:"
        rn = 1
        while True:
            num_swept = 0
            # We iteration throught the LRU list backwards, garbages are more
            # likely to be situated at the most recent end. This heuristic
            # only works for Python because of its reference counting GC.
            for le in list_prev_iter(self._lrulist):
                ce = le.data
                coid = ce.coidwref()
                if not coid:
                    self._delcentry(ce)
                    num_swept += 1
            # Stop when there is no garbages left
            #print "Round %d: %d swept" % (rn, num_swept)
            rn += 1
            if num_swept == 0:
                break
            total_swept += num_swept
        self._last_swept = total_swept
        #print "Sweep: %d rounds %d dead coids" % (rn, total_swept)
        if _cprof:
            _cprof.sweepcnt += 1
            _cprof.deadcoid1cnt += total_swept
        return total_swept

    def dump_lrulist(self):
        print "LRU List: [",
        for e in list_next_iter(self._lrulist):
            centry = e.data
            s = centry.ofields[0]
            if s == "":
                s = '@'
            if not centry.coidwref():
                s = "~%s(%d)" % (s, centry.seqnum)
            print s,
        print "]"

    def _flush_lru_entry(self):
        ''' Free up some cache entries by "flusing" the least recently used
        entries to PStor '''
        # least recently used entries are from head of list
        if _cprof:
            _cprof.fullcnt += 1
        if list_empty(self._lrulist):
            return
        # LRU entry is next of list head
        node = self._lrulist._next
        centry = node.data
        self._flush(centry)

    def _flush(self, centry):
        ''' Flushes a cache entry ''centry'' to PStor and delete the entry. '''
        coid = centry.coidwref()
        if coid:
            # coid is valid, write-through to PStor
            self._write_coid(coid)
        else:
            # the coid is garbage.
            if _cprof:
                _cprof.deadcoid2cnt += 1
            #print "Flush: Seeing garbage coid %d" % centry.seqnum
            pass
        # Delete cache entry
        self._delcentry(centry)
        #print "Flushed centry %d" % centry.seqnum

    def _write_coid(self, coid):
        ''' Write the cached oid ''coid'' to PStor. Return the resulting OID.
        If a field in the coid refers to another coid, that coid will be
        written first. As a result, a tree of oids are written through by
        this function. Side effect: ''oid'' field of the coid is added
        as a result. - The ''coid'' now has a "backing" OID. '''
        # Nulloid is never cached and is used as if it is a cached oid.
        if coid is oid.OID.Nulloid:
            return coid
        if coid.oid is not None:
            # This coid has already been written (to PStor). It won't ever
            # change.
            return coid.oid
        # Now this coid MUST be in cache, otherwise it would be a "phantom"
        # coid...
        assert(coid.seqnum in self._cache)
        centry = self._cache[coid.seqnum]
        # ''ofields'' MUST contain only native Python objects or a "real" OID
        ofields = centry.ofields[:]
        #print "Writing coid %d: %s" % (coid.seqnum, ofields)
        for i, f in enumerate(ofields):
            if isinstance(f, _CachedOid):
                # Note that we substitute a coid with the "real" OID created
                # through _write_coid. Also note that the original coid is
                # modified (added a ''oid'').
                ofields[i] = self._write_coid(f)
        o = coid.pstor.create(ofields)
        assert(hasattr(coid, "name"))
        ps = persistds.PStruct.mkpstruct(coid.name)
        ps.initOid(o)
        coid.oid = o
        if _cprof:
            _cprof.wtcnt += 1
        return coid.oid

    def _write_all_coids(self):
        ''' Collect garbage and write out all coids. '''
        self._sweep_garbage()
        # Now there is no more garbage. We flush out all coids.
        for le in list_next_iter(self._lrulist):
            ce = le.data
            coid = ce.coidwref()
            assert(coid)
            self._write_coid(coid)

    def close(self):
        ''' Destroys cache '''
        self._write_all_coids()
        self._cache = {}
        _list_init(self._lrulist)

    def create(self, ofields, pstor):
        ''' Interface to PersistDS's OID create '''
        coid = _CachedOid(pstor)
        self._add(coid, ofields)
        return coid

    def _cache_oid(self, o):
        ''' Load an OID ''o'' from PStor and add it to our cache. Return the
        resulting coid. '''
        # oid.pstor is a string
        pstor = pstructstor.PStructStor.mkpstor(o.pstor)
        ofields = pstor.getrec(o)
        # Create a _CachedOid based on the real OID
        coid = _CachedOid(pstor, o)
        # Have to give it a name
        ps = persistds.PStruct.mkpstruct(o.name)
        ps.initOid(coid)
        # Put it back into cache. Note is possible that multiple coids refer
        # to the same underlying OID as a result of doing this. If we maintain
        # a reverse hash (oid -> coid) then this can be avoided.
        self._add(coid, ofields)
        if _cprof:
            _cprof.coldcnt += 1
        return coid

    def _cache_ofields(self, ofields):
        ''' Goes through ''ofields'', look for fields that are of type OID,
        creates a coid based on that oid and replaces the field with the newly
        created coid. This is done because the following reason:
        When returning a coid to running program, we need to cache any oid
        in the ''ofields'' of the coid because we don't
        want caller to get hold of a "real" oid. If we let user to have
        a "real" oid then that oid will ALWAYS have to be "cold loaded"
        from PStor, even if we "cache" that oid internally. Why? because
        the internally cached coid is not accessible to the user - we
        cannot change the Python reference that point to the oid to
        instead point to our coid. This little procedure transforms
        any OID field within ofields to a COID. '''
        for i, f in enumerate(ofields):
            if isinstance(f, oid.OID) and f is not oid.OID.Nulloid:
                ofields[i] = self._cache_oid(f)
        return ofields

    def _coid_from_oid(self, o):
        ''' Creates a new coid from a oid. You need call this function
        if you don't already have a coid - this implies that the oid ''o''
        is a previously saved oid. '''
        # Nulloid is not cached.
        if o is oid.OID.Nulloid:
            return o
        coid = self._cache_oid(o)
        centry = _pdscache._cache[coid.seqnum]
        _pdscache._cache_ofields(centry.ofields)
        return coid

    def _get_coidrec(self, coid):
        ''' Interface to PersistDS's OID getrec when the passed oid is a
        cached oid ''coid''. '''
        try:
            centry = self._cache[coid.seqnum]
            # Coid in cache: Move it to the tail of LRU list
            list_move_tail(centry.lnode, self._lrulist)
            #print "Getting centry %d" % centry.seqnum
            if _cprof:
                _cprof.hitcnt += 1
            return self._cache_ofields(centry.ofields)
        except KeyError:
            # This Oid Cache has been moved to pstor, we have to get it back
            # first
            #print "Getting coid (%d) from PStor" % coid.seqnum
            assert(coid.oid is not None)
            ofields = coid.pstor.getrec(coid.oid)
            # Put the coid back into cache. Note coid.seqnum is reused here.
            self._add(coid, ofields)
            if _cprof:
                _cprof.misscnt += 1
            return self._cache_ofields(ofields)


import os
import datetime
class _CacheProf(object):
    def reset_stats(self):
        self.coidcnt = 0
        self.centrycnt = 0
        self.fullcnt = 0
        self.wtcnt = 0
        self.coldcnt = 0
        self.hitcnt = 0
        self.misscnt = 0
        self.sweepcnt = 0
        self.deadcoid1cnt = 0
        self.deadcoid2cnt = 0
 
    def __init__(self):
        self.clock = 0
        self.reset_stats()
        fpath = "cacheprof.stats-%d-%s" % (_pdscache_size,
                                           datetime.date.today().isoformat())
        self.statsfo = open(fpath, "w")
        fmtstr = "%13s" * 11 + "\n"
        self.statsfo.write(fmtstr % \
                               ("clock",
                                "coidcnt",
                                "centrycnt",
                                "fullcnt",
                                "wtcnt",
                                "coldcnt",
                                "hitcnt",
                                "misscnt",
                                "sweepcnt",
                                "deadcoid1cnt",
                                "deadcoid2cnt"))

    def tick(self):
        self.clock += 1
        if self.clock % 1000 == 0:
            self.calc()
            self.reset_stats()
            #print "%s: " % self.clock,
            #_pdscache.dump_lrulist()

    def calc(self):
        fmtstr = "%13d" * 11 + "\n"
        self.statsfo.write(fmtstr % \
                               (self.clock,
                                self.coidcnt,
                                self.centrycnt,
                                self.fullcnt,
                                self.wtcnt,
                                self.coldcnt,
                                self.hitcnt,
                                self.misscnt,
                                self.sweepcnt,
                                self.deadcoid1cnt,
                                self.deadcoid2cnt))

# Cache stats
#_cprof = _CacheProf()
_cprof = None

# Cache Size (Number of cache entries)
_pdscache_size = 4096
# This is the global singleton PDS cache object that everyone uses.
_pdscache = PDSCache(_pdscache_size)
print "PDSCache %s: Size %d" % (_pdscache, _pdscache_size)

# Cache Management
def write_coid(coid):
    ''' Write through a cached oid ''coid''. Return the resulting oid. '''
    return _pdscache._write_coid(coid)

def read_oid(o):
    ''' Read (load) an oid ''o'' from pstor. Return the resulting coid. '''
    return _pdscache._coid_from_oid(o)


# Interface to PStructStor
# Use these public functions to create and get OIDs. These functions are
# inserted between persistds.PStruct and pstructstor.PStructStor.
def create_oid(ofields, pstor):
    ''' Create a cached OID. '''
    coid = _pdscache.create(ofields, pstor)
    if _cprof:
        _cprof.tick()
    return coid

def oidfields(coid):
    ''' Return fields of a coid. '''
    if not isinstance(coid, _CachedOid):
        raise TypeError("Wrong type: %s of %s. Must be _CachedOid" % \
                            (coid, type(coid)))
    ofields = _pdscache._get_coidrec(coid)
    if _cprof:
        _cprof.tick()
    return ofields
