#!/usr/bin/env python

import os
from persistds import PStruct
import pstructstor
from oid import OID

#
# Global trie node PStruct
# The default values makes a trie root
# Fields:
# prefix:       The prefix of text for that trie node. This is essentially 
#               the "path" to the node.
# final:        If final is True, then the prefix is also the actual text. Note
#               that a final node is different from a leaf node.
# lcp:          The "Left Child Pointer".
# rsp:          The "Right Sibling Pointer"
#

Nulltrie = OID.Nulloid

_default_tnode_fields = (
        ('prefix', ''),
        ('value', None),
        ('final', False),
        ('lcp', Nulltrie),
        ('rsp', Nulltrie),);

ptrieStruct = PStruct.mkpstruct('trienode', _default_tnode_fields)

class Ptrie(object):
    def __init__(self, pstor):
        self.pstor = pstor
        
    def makeTnode(self, **kwargs):
        return ptrieStruct.make(self.pstor, **kwargs)

    def getfields(self, oid):
        return ptrieStruct.getfields(self.pstor, oid)

    # A trie is constructed via insertions
    def insert(self, trie, key, value=None):
        ''' Insert a (key, value) as a child node into the trie at parent.
        key is a text key, trie is organized based on the text key '''
        print "Insert: '%s'" % key
        return self.orderedInsert(trie, key, value, 0)

    ##
    # This is the recursive function illustrating the gist of the algorithm
    #
    def orderedInsert(self, head, key, value, endpos):
        ''' Inserts a sibling node into head ordered by the letter
        at (@endpos - 1). key[0:endpos] will become the 
        prefix for the new node. Returns the new head (not the new node) '''
        #print '>>>>>>>>>"%s": endpos %d:' % (key, endpos)
        if endpos > len(key):
            #print '<<<<==%s==<<<<' % head
            return head

        def intrmedValue(final):
            if final:
                return value
            else:
                return None

        final = (endpos == len(key))
        prefix = key[0:endpos]
        if head is Nulltrie:
            #print "Creating HEAD:"
            lcp = self.orderedInsert(Nulltrie, key, value, endpos + 1)
            res = self.makeTnode(prefix=prefix, final=final,
                                 value=intrmedValue(final), lcp=lcp)
            #print '<<<<<==%s==<<<<<' % res
            return res
        headfds = self.getfields(head)
        assert(endpos == len(headfds['prefix']))
        # string[-1:0] is an empty string so this works fine for endpos == 0
        newkey = key[endpos-1:endpos]
        headkey = headfds['prefix'][endpos-1:endpos]
        diff = cmp(newkey, headkey)
        #print 'key %s headkey %s' % (newkey, headkey)
        if diff == 0:
            # Same prefix, but still need to update final and new children
            lcp = self.orderedInsert(headfds['lcp'], key, value, endpos + 1)
            # If head was final then it needs to remain final
            final = final or headfds['final']
            res = self.makeTnode(prefix=prefix, final=final,
                                 value=intrmedValue(final),
                                 lcp=lcp,
                                 rsp=headfds['rsp'])
            #print '<<<<==%s==<<<<<<' % res
            return res
        elif diff < 0:
            # newkey is smaller, should insert before head
            lcp = self.orderedInsert(Nulltrie, key, value, endpos + 1)
            res = self.makeTnode(prefix=prefix, final=final,
                                 value=intrmedValue(final),
                                 lcp=lcp, rsp=head)
            #print '<<<<<==%s==<<<<<' % res
            return res
        else:
            # newkey is bigger, insert into head's rsp => newrsp
            newrsp = self.orderedInsert(headfds['rsp'], key, value, endpos)
            res = self.makeTnode(prefix=headfds['prefix'],
                                 final=headfds['final'],
                                 value=intrmedValue(final),
                                 lcp=headfds['lcp'], rsp=newrsp)
            #print '<<<<<==%s==<<<<<' % res
            return res

    def dfSearch(self, trie, func):
        """ Traverses the trie in depth-first order """
        if trie is Nulltrie:
            return
        self._dfs([trie], func)

    def _dfs(self, rest, func):
        ''' iterative version of dfs '''
        while len(rest):
            curr = rest.pop()
            func(curr)
            f = self.getfields(curr)
            if f['rsp']:
                rest.append(f['rsp'])
            if f['lcp']:
                rest.append(f['lcp'])

    def bfsearch(self, trie, func):
        ''' Traverses trie in breadth-first order '''
        if trie is Nulltrie:
            return
        return self._bfs([trie], func)

    def _bfs(self, rest, func):
        ''' Iterative version of bf search '''
        while len(rest):
            curr = rest.pop(0)
            func(curr)
            fields = self.getfields(curr)
            if fields['lcp']:
                rest.append(fields['lcp'])
            if fields['rsp']:
                rest.insert(0, fields['rsp'])

    def find(self, trie, key, finalOnly=True):
        ''' Find a Ptrie node with the "final" prefix of key. '''
        if not trie:
            return None
        fields = self.getfields(trie)
        # findByPosition assumes that search positions for key and trie node
        # matches
        if len(fields['prefix']) != 0:
            raise RuntimeError("Search must start from root")
        return self.findByPosition(trie, key, 0, finalOnly)

    @staticmethod
    def markstrpos(string, endpos):
        ''' Marks the character at endpos-1 with a pair of parens '''
        if endpos < 0 or endpos > len(string):
            raise KeyError("endpos %d is out of range for %s"
                           % (endpos, string))
        if endpos == 0:
            return ("()" + string)
        front = string[:endpos-1]
        mid = "(" + string[endpos-1:endpos] + ")"
        back = string[endpos:]
        return front + mid + back
                           
    def findByPosition(self, trie, key, endpos, finalOnly):
        ''' Find a trie node whose prefix at position endpos-1 is equal to
        key[endpos-1], assuming that key[0:endpos] already matched previous
        trie nodes in the path '''
        #print "findByPosition(%s %s %d)" % (trie, key, endpos)
        if not trie:
            # This means we are at the end of a sibling chain without
            # having found a match
            return Nulltrie
        targetkey = key[endpos-1:endpos]
        fields = self.getfields(trie)
        triekey = fields['prefix'][endpos-1:endpos]
        #print "%s <=> %s" % (Ptrie.markstrpos(key, endpos), fields['prefix'])
        if targetkey == triekey:
            # This position is a match, now move on the next position, starting
            # from the first child of this trie node.
            if endpos == len(key):
                # endpos is the last position: the whole key is matched
                if (not finalOnly) or fields['final']:
                    return trie
                else:
                    return Nulltrie
            return self.findByPosition(fields['lcp'], key, endpos+1, finalOnly)
        elif targetkey > triekey:
            # no match yet, go down the sibling chain and try again.
            return self.findByPosition(fields['rsp'], key, endpos, finalOnly)
        else:
            # siblings are ordered by key, so this means the target key is not
            # present
            return Nulltrie

    def delete(self, trie, key):
        ''' Deletes a trie node whose prefix matches key from the trie. '''
        return self.deleteByPosition(trie, key, 0)
    
    # Recursive
    def deleteByPosition(self, trie, key, endpos):
        ''' Deletes a trie node whose prefix[endpos:] matches key[endpos:].
        This means that the key[:endpos] already matches the partial trie
        so far '''
        if not trie:
            return Nulltrie
        targetkey = key[endpos-1:endpos]
        fields = self.getfields(trie)
        triekey = fields['prefix'][endpos-1:endpos]
        if targetkey == triekey:
            if endpos == len(key):
                # whole key matched.
                if not fields['final']:
                    # Not found, trie is left unchanged
                    return trie
                # This is true match. Now if the trie node has children,
                # then the node is kept but its 'final' bit is marked off.
                # Otherwise remove the node and returns its right sibling.
                if fields['lcp']:
                    return self.makeTnode(
                        prefix=fields['prefix'],
                        value=fields['value'],
                        final=False,
                        lcp=fields['lcp'],
                        rsp=fields['rsp'])
                else:
                    return fields['rsp']
            else:
                # search children with the next position
                newtrie = self.deleteByPosition(fields['lcp'], key, endpos+1)
                if newtrie is fields['lcp']:
                    # nothing deleted
                    return trie
                else:
                    return self.makeTnode(
                        prefix=fields['prefix'],
                        value=fields['value'],
                        final=fields['final'],
                        lcp=newtrie,
                        rsp=fields['rsp'])
        elif targetkey > triekey:
            # Keep going down the sibling chain
            newtrie = self.deleteByPosition(fields['rsp'], key, endpos)
            if newtrie is fields['rsp']:
                return trie
            else:
                return self.makeTnode(
                    prefix=fields['prefix'],
                    value=fields['value'],
                    final=fields['final'],
                    lcp=fields['lcp'],
                    rsp=newtrie)
        else:
            # not found
            return trie

