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


# This program enumerates permutations of n pancakes and the paths through which
# a particular permutation can be sorted by prefix reversal, or "flipping".
# See http://en.wikipedia.org/wiki/Pancake_sorting for details.
#
# Algorithm
# A permutation of n pancakes is represented by a string of n numbers (1 - n),
# where a number represents the rank of the pancake. For example, "1234"
# represents the sorted 4-pancake stack - "1" is the smallest and "4" is the
# largest.
#
# The program starts from the sorted sequence, doing one prefix reversal at a
# time and inserts the resulting sequences into a trie of level k. For example,
# Starting from "1234", with no flipping, sequence "1234" is inserted into
# trie of level 0 (pancake-trie-0). Proceeding to the next level, with one
# flip, "2134", "3214" and "4321" are produced, each one of these are inserted
# into level 1 (pancake-trie-1). The step continues for each of sequence at
# the previous level, resulting pancake tries of levels from 0 to max-flip(n),
# where max-flip(n) is the number of flips required to sort those sequences of n
# pancakes that require the maximum of the flips.
#
# Data structures
# * pancakeTrieList
# List of pancakes tries, the first (head of list) trie is of
# level 0, the second is level 1, and so on.
# * pancakeTrie
# Trie of sequences of pancake stack.


import sys
import os
import ostore
import plist
import ptrie
import pdscache
from oidfs import OidFS


class Pancake(object):
    ''' A pancake stack of ''numpcakes'' pancakes '''

    def __init__(self, numpcakes, pstor, ofs):
        if numpcakes <= 0 or numpcakes > 35:
            raise ValueError("numpcakes must be between 1 and 35")
        self.pstor, self.ofs = pstor, ofs
        self.plistObj = plist.Plist(pstor)
        self.ptrieObj = ptrie.Ptrie(pstor)
        self.numpcakes = numpcakes
        self.pcakeTrieList = plist.emptylist

    @staticmethod
    def sortedSeq(n):
        ''' Returns a string of sequence from 1 to n. Because decimal system only has
        10 digits and n can be more than 9. We use letter 'a' for 10, 'b' for 11,
        and so on. This way we can address up to 9 + 26 = 35 pancakes. '''
        seq = ""
        for i in range(1, n + 1):
            if i < 10:
                s = "%d" % i
            else:
                asciiv = ord('a') + (i - 10)
                s = chr(asciiv)
            seq += ("%s" % s)
        return seq

    def _mkPcakeTrie(self, curlist):
        ''' Create a new ptrie ''pcakeTrie'' from the previous level. The first
        level is the sorted sequence. ''curlist'' is the current list of pancake
        ptries. '''
        if (curlist is plist.emptylist):
            # Level 0 is simply the sorted sequence
            s = Pancake.sortedSeq(self.numpcakes)
            print "    <%s>" % s
            print "  1 Total"
            return self.ptrieObj.insert(ptrie.Nulltrie, s, None)
        # Create current level from the previous level
        cnt = 0
        pcakeTrie = ptrie.Nulltrie
        prevTrie = self.plistObj.car(curlist)
        for node in self.ptrieObj.bfiter(prevTrie):
            f = self.ptrieObj.getfields(node)
            if f['final']:
                prevseq = f['prefix']
                #print "Flipping prevseq '%s'" % prevseq
                # Now do prefix reversals on ''prevseq'' and insert the
                # resulting sequence into ''pcakeTrie'' if it is not a duplicate.
                for seq in Pancake.prefix_rev_iter(prevseq):
                    # Insert seq if not a duplicate
                    if not self._isDup(seq, curlist):
                        #pt = self.ptrieObj.insert(pcakeTrie, seq, prevseq, None)
                        pt = self.ptrieObj.insert(pcakeTrie, seq, None, None)
                        if pt is not pcakeTrie:
                            #print "    <%s>" % seq
                            pcakeTrie = pt
                            cnt += 1
        print "  %d Total" % cnt
        return pcakeTrie

    def _isDup(self, seq, ptlist):
        ''' Check if ''seq'' exists in ptrie list ''ptlist''. '''
        for pt in self.plistObj.liter(ptlist):
            #print "Checking Ptrie %s for seq '%s'" % (pt, seq)
            if self.ptrieObj.find(pt, seq):
                # Found
                return True
        return False

    @staticmethod
    def prefix_rev_iter(tmpl):
        ''' Make new sequences by doing prefix reversal on ''tmpl''. This is an
        iterator. '''
        length = len(tmpl)
        assert(length > 0)
        if length == 1:
            yield tmpl
            return
        for i in range(2, length + 1):
            head = tmpl[:i]
            tail = tmpl[i:]
            yield (head[::-1] + tail)

    def build(self):
        ''' Builds the pancake ptrie list one level at a time. Build stops when
        a 'pcakeTrie'' is empty, which means that there are no new sequences left,
        in other words, we have enumerated all permutations of the n pancakes. '''
        i = 0
        while True:
            print "Level %d" % i
            pcakeTrie = self._mkPcakeTrie(self.pcakeTrieList)
            if pcakeTrie is ptrie.Nulltrie:
                break
            i += 1
            self.pcakeTrieList = self.plistObj.cons(pcakeTrie, self.pcakeTrieList)


if __name__ == "__main__":
    import sys

    num = int(sys.argv[1])
    pstor, ofs = ostore.init_ostore()
    pcakeObj = Pancake(num, pstor, ofs)
    pcakeObj.build()
    ofs.close()
    pstor.close()
