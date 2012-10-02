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
import persistds
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

ptrieStruct = persistds.PStruct.mkpstruct('trienode', _default_tnode_fields)

def replace_value(v1, v2):
    return v2

class Ptrie(object):
    def __init__(self, pstor):
        self.pstor = pstor
        
    def makeTnode(self, **kwargs):
        return ptrieStruct.make(self.pstor, **kwargs)

    def getfields(self, oid):
        return ptrieStruct.getfields(self.pstor, oid)

    # A trie is constructed via insertions
    def insert(self, trie, key, value, mergevalue=replace_value):
        ''' Insert a (key, value) as a child node into ''trie''.
        ''key'' is a text key, trie is organized based on the text key.
        If key exists then the nodes are "merged" '''
        #print "Insert: '%s'" % key
        fields = {'prefix': key, 'value': value, 'final': True,
                  'lcp': Nulltrie, 'rsp': Nulltrie}
        if not trie:
            return self._create_trie_branch(0, fields)
        return self._insert(trie, fields, mergevalue)

    def _insert(self, trie, nodefields, mergevalue):
        ''' Insert a trie node with ''nodefields'' into a trie. '''
        pfinder = PtriePathFinder(self, trie)
        pfinder.search(nodefields['prefix'])
        #print pfinder.path
        # The last of node of the search path is either a trie node if the key
        # exists, or Nulltrie if key doesn't.
        if pfinder.target:
            # key exists, needs merging.
            newnode = self.makeTnode(
                prefix=nodefields['prefix'],
                value=nodefields['value'],
                final=nodefields['final'],
                lcp=nodefields['lcp'],
                rsp=nodefields['rsp'])
            if mergevalue is None:
                return trie
            newnode = self._merge_tnodes(pfinder.target, newnode, mergevalue)
        else:
            # Need to create a new trie node
            pn, rel = PtriePathFinder.decode_path_mark(pfinder.path[0])
            pn_f = self.getfields(pn)
            if rel == 'rsp':
                # new node is on the sibling chain
                pos = len(pn_f['prefix'])
                rsp = pn_f['rsp']
            elif rel == 'lcp':
                # new node is the lcp
                pos = len(pn_f['prefix']) + 1
                rsp = pn_f['lcp']
            newnode = self._create_trie_branch(pos, nodefields)
            # Now append the rsp determined from above to the newnode
            fields = self.getfields(newnode)
            newnode = self.makeTnode(
                prefix=fields['prefix'],
                value=fields['value'],
                final=fields['final'],
                lcp=fields['lcp'],
                rsp=rsp)
        # Now reconstruct the search path
        return pfinder.retrace(newnode)

    def merge_trie(self, t1, t2, mergevalue=replace_value):
        ''' Merges two tries into one '''
        if not t1: return t2
        if not t2: return t1
        f1 = self.getfields(t1)
        f2 = self.getfields(t2)
        # The trie with the shorter prefix is the "parent" trie (tp), the one
        # with the longer prefix will be the "child" trie (tc). tc will be
        # inserted into tp.
        if len(f1['prefix']) < len(f2['prefix']):
            tp = t1; fp = f1; tc = t2; fc = f2
        elif len(f2['prefix']) < len(f1['prefix']):
            tp = t2; fp = f2; tc = t1; fc = f1
        else:
            if f1['prefix'] == f2['prefix']:
                return self._merge_tnodes(t1, t2, mergevalue)
            elif f1['prefix'] < f2['prefix']:
                tp = t1; fp = f1; tc = t2; fc = f2
            else:
                tp = t2; fp = f2; tc = t1; fc = f1
        # height(tc) >= height(tp)
        return self._insert(tp, fc, mergevalue)
        
    def _merge_tnodes(self, tn1, tn2, mergevalue=replace_value):
        ''' Merges tries tn1 and tn2 into one. tn1 and tn2 must have the same
        prefix (key) '''
        f1 = self.getfields(tn1)
        f2 = self.getfields(tn2)
        assert(f1['prefix'] == f2['prefix'])
        final = f1['final'] or f2['final']
        # Merge old value with new value in mergevalue(oldval, newval)
        if f1['final'] and f2['final']:
            value = mergevalue(f1['value'], f2['value'])
        elif f1['final']:
            value = f1['value']
        elif f2['final']:
            value = f2['value']
        else:
            value = None
        rsp = self.merge_trie(f1['rsp'], f2['rsp'], mergevalue)
        lcp = self.merge_trie(f1['lcp'], f2['lcp'], mergevalue)
        return self.makeTnode(prefix=f1['prefix'], value=value, final=final,
                              lcp=lcp, rsp=rsp)

    def _create_trie_branch(self, pos, fields):
        ''' Creat a trie branch whose leaf is specified by ''fields''. Branch
        creation starts from ''pos'' of the prefix of the leaf node. '''
        key = fields['prefix']
        if pos > len(key):
            raise ValueError("pos %d is out of bounds for key %s" % (pos, key))
        # Create leaf node first, then work backwards
        node = fields['lcp']
        p = len(key)
        while p >= pos:
            val = None
            final = False
            rsp = Nulltrie
            if p == len(key): # leaf node
                val = fields['value']
                final = fields['final']
                rsp = fields['rsp']
            node = self.makeTnode(prefix=key[0:p], value=val, final=final,
                                  lcp=node, rsp=rsp)
            p -= 1
        return node

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

    def dfiter(self, trie):
        """ An iterator that traverses the trie in depth-first order """
        if trie is Nulltrie:
            return
        for node in self._dfs([trie]):
            yield node

    def _dfs(self, rest):
        ''' iterative version of dfs '''
        while len(rest):
            curr = rest.pop()
            yield curr
            f = self.getfields(curr)
            if f['rsp']:
                rest.append(f['rsp'])
            if f['lcp']:
                rest.append(f['lcp'])

    def bfiter(self, trie):
        ''' Iterate trie in breadth-first order '''
        if trie is Nulltrie:
            return
        for node in self._bfs([trie]):
            yield node

    def _bfs(self, rest):
        ''' Iterative version of bf search '''
        while len(rest):
            curr = rest.pop(0)
            yield curr
            fields = self.getfields(curr)
            if fields['lcp']:
                rest.append(fields['lcp'])
            if fields['rsp']:
                rest.insert(0, fields['rsp'])

    def find(self, trie, key, finalOnly=True):
        ''' Find a Ptrie node with the "final" prefix of key. '''
        if not trie:
            return Nulltrie
        fields = self.getfields(trie)
        if len(fields['prefix']) != 0:
            raise RuntimeError("Search must start from root")
        pfinder = PtriePathFinder(self, trie)
        pfinder.search(key)
        #print pfinder.path
        if pfinder.target:
            fields = self.getfields(pfinder.target)
            if (not finalOnly) or fields['final']:
                return pfinder.target
        return Nulltrie

    def delete(self, trie, key):
        ''' Deletes a trie node whose prefix matches key from the trie. '''
        pfinder = PtriePathFinder(self, trie)
        pfinder.search(key)
        if not pfinder.target: # not found, no change
            return trie
        # Need to reconstruct new trie with the target removed
        fields = self.getfields(pfinder.target)
        if not fields['final']: # This is an intermediate node, doesn't count
            return trie
        # This is true match. Now if the trie node has children,
        # then the node is kept but its 'final' bit is marked off and the
        # ''value'' field is set to None. Otherwise remove the node and
        # returns its right sibling.
        if fields['lcp']:
            newnode = self.makeTnode(
                prefix=fields['prefix'],
                value=None,
                final=False,
                lcp=fields['lcp'],
                rsp=fields['rsp'])
        else:
            newnode = fields['rsp']
        # Delete any "hanging" branch - an internal (non-final) node with
        # no children
        while not newnode:
            # Go up the path
            if len(pfinder.path) == 0:
                break
            # Peek at path head
            pmark = pfinder.path[0]
            predecessor, rel = PtriePathFinder.decode_path_mark(pmark)
            fields = self.getfields(predecessor)
#            print "<%s -> %s> (lcp %s rsp %s)" % \
#                (fields['prefix'], rel, fields['lcp'], fields['rsp'])
            # Predecessor is either
            # 1) a final node or
            # 2) an internal node with an lcp and that lcp is not the delete
            #    target (if delete target is the lcp of predecessor then
            #    fields['lcp'] points to the "old" deletion target)
            if fields['final'] or (rel != 'lcp' and fields['lcp']):
                break # stop here
            # Predecessor is an internal node with no children
            newnode = fields['rsp']
            pfinder.path.pop(0)
        return pfinder.retrace(newnode)
            
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


class PtriePathFinder(object):
    ''' A Helper class that specializes in finding a node in a ptrie and saves
    the path leading to the node. Also helps in reconstructing a new ptrie
    if the ptrie is changed '''
    def __init__(self, ptrieObj, tnode):
        self._ptrieObj = ptrieObj
        self._root = tnode

    def search(self, key):
        ''' Searches and establishes a search path leading to the target. '''
        self.path = []
        self.target = Nulltrie
        if not self._root:
            # Empty trie: return empty path.
            return
        fields = self._ptrieObj.getfields(self._root)
        startpos = len(fields['prefix'])
        if startpos > len(key):
            raise ValueError("Key length must be at least that of the root (%d)"
                             % startpos)
        # Path is a reversed list of nodes on the search path
        trie = self._root
        # ''pos'' is the end position of the key that is matched so far. In
        # other words, key[0:pos] is already matched.
        pos = startpos
        while pos <= len(key):
            #print "pos: %d trie: %s" % (pos, trie)
            if not trie:
                return
            targetkey = key[pos-1:pos]
            fields = self._ptrieObj.getfields(trie)
            triekey = fields['prefix'][pos-1:pos]
            #print "%s <=> %s" % (PtriePathFinder.markstrpos(key, pos), fields['prefix'])
            if targetkey == triekey:
                # This position is a match, now move on the next position,
                #starting from the first child of this trie node.
                self.path.insert(0, PtriePathFinder.make_path_mark(
                        trie, "lcp"))
                trie = fields['lcp']
                pos += 1
            elif targetkey > triekey:
                # Search next sibling at the same position
                self.path.insert(0, PtriePathFinder.make_path_mark(
                        trie, "rsp"))
                trie = fields['rsp']
            else:
                # siblings are ordered by key, so this means the target key is
                # not present
                return
        # If a match is found, remove it from the path and save it
        self.target, rel = self.path.pop(0)

    def retrace(self, newnode):
        ''' retrace back the search path and reconstruct a new trie along the
        way. ''newnode'' is the new trie node at the end of the path (But will
        be attached to the new trie first). '''
        newroot = newnode
        for pmark in self.path:
            pn, rel = PtriePathFinder.decode_path_mark(pmark)
            pn_f = self._ptrieObj.getfields(pn)
            if rel == 'rsp':
                newroot = self._ptrieObj.makeTnode(
                    prefix=pn_f['prefix'],
                    value=pn_f['value'],
                    final=pn_f['final'],
                    lcp=pn_f['lcp'],
                    rsp=newroot)
            elif rel == 'lcp':
                newroot = self._ptrieObj.makeTnode(
                    prefix=pn_f['prefix'],
                    value=pn_f['value'],
                    final=pn_f['final'],
                    lcp=newroot,
                    rsp=pn_f['rsp'])
            else:
                raise RuntimeError("Invalid Path Marker: %s" % rel)
        return newroot

    @staticmethod
    def make_path_mark(node, next):
        return (node, next,)

    @staticmethod
    def decode_path_mark(pm):
        return (pm[0], pm[1],)

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

