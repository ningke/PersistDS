import time
import random
import ostore
import ptrie
import pdscache
from oidfs import OidFS


def rand_perm(txt):
    ''' Iterates random permutations of ''txt''. '''
    txtlen = len(txt)
    if txtlen == 0:
        yield txt
        return
    for s in rand_perm(txt[1:]):
        randpos = range(0, txtlen)
        random.shuffle(randpos)
        for i in randpos:
            yield s[:i] + txt[0] + s[i:]

def sortedSeq(n):
    ''' Returns a string of sequence from 1 to n. Because decimal system only has
    10 digits and n can be more than 9. We use letter 'a' for 10, 'b' for 11,
    and so on. This way we can address up to 9 + 26 = 35 pancakes. '''
    if n > 35:
        raise ValueError("Sorry, cannot do more than 35.")
    seq = ""
    for i in range(1, n + 1):
        if i < 10:
            s = "%d" % i
        else:
            asciiv = ord('a') + (i - 10)
            s = chr(asciiv)
        seq += ("%s" % s)
    return seq

class Perm(object):
    def __init__(self):
        self.pstor, self.ofs = ostore.init_ostore()
        self.ptrieObj = ptrie.Ptrie(self.pstor)
        self.root = ptrie.Nulltrie # Start with Nulltrie as root
        self.count = 0

    def insert(self, seq):
#        # Do GC periodically
#        if self.count > 0 and self.count % 5313 == 0:
#            before = time.clock()
#            print "Doing GC (%d perms so far)" % self.count
#            self.pstor.print_stats()
#            rootOid = pdscache.write_coid(self.root)
#            rootOid, = self.pstor.keepOids([rootOid,])
#            self.root = pdscache.read_oid(rootOid)
#            print "GC took %d seconds" % (time.clock() - before)
        self.root = self.ptrieObj.insert(self.root, seq, None, None)
        self.count += 1

    def bfwalk(self):
        before = time.clock()
        for node in self.ptrieObj.bfiter(self.root):
            pass
        print "BF trie walk took %f seconds" % (time.clock() - before)
        self.pstor.print_stats()

    def inspect(self):
        myname = "random_permutations"
        print "bfwalk 1:"
        self.bfwalk()
        before = time.clock()
        self.ofs.store(self.root, myname)
        print "Storing took %f seconds" % (time.clock() - before)
        self.pstor.print_stats()
        print "Doing GC"
        before = time.clock()
        self.ofs.gc()
        print "GC took %f seconds" % (time.clock() - before)
        self.root = self.ofs.load(myname)
        print "bfwalk 2:"
        self.bfwalk()
        print "bfwalk 3:"
        self.bfwalk()

    def close(self):
        self.ofs.store(self.root, "random_permutations")
        self.ofs.gc()
        self.ofs.close()
        self.pstor.close()


if __name__ == "__main__":
    import sys
    import math

    if len(sys.argv) < 2:
        print "%s: number" % (sys.argv[0])
        exit(0)
    n = int(sys.argv[1])
    seq = sortedSeq(n)
    pm = Perm()
    before = time.clock()
    for s in rand_perm(seq):
        #print "Inserting '%s'" % s
        pm.insert(s)
    print "%d permutations total. %d seconds" % \
        (math.factorial(n), time.clock() - before)
    pm.inspect()
    pm.close()
