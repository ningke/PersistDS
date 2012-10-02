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


from fixszPDS import *
from persistds import PStruct
from oid import OID

# Global list node PStruct
emptylist = OID.Nulloid
nodePS = PStruct.mkpstruct('node',
        (('value', None), ('nxt', emptylist)))

# So a list node is represented by an oid created by nodePS.make(). Use
# the cons function to contruction the list.
class Plist(object):
    ''' Plist class is simply a wrapper for nodePS. It is initialized with a
    PStructStor so OIDs can be made in somewhere. '''
    
    def __init__(self, pstor):
        ''' Initialize with a PStructStor. All nodePS OIDs will be made in
        that pstor '''
        self.pstor = pstor
        
    def cons(self, val, lnode):
        return nodePS.make(self.pstor, value=val, nxt=lnode)

    def car(self, lnode):
        if not lnode:
            raise ValueError("car: emptylist")
        fields = nodePS.getfields(self.pstor, lnode)
        return fields['value']

    def cdr(self, lnode):
        if not lnode:
            raise ValueError("cdr: emptylist")
        fields = nodePS.getfields(self.pstor, lnode)
        return fields['nxt']

    def plist(self, *args):
        ''' A plist constructor that takes variable number of values and make
        them into a plist '''
        if len(args) == 0:
            return emptylist
        return nodePS.make(self.pstor, value=args[0], nxt=self.plist(*args[1:]))

    def liter(self, ll):
        ''' returns a generator of the linked list '''
        while ll is not emptylist:
            yield self.car(ll)
            ll = self.cdr(ll)

    def map(self, func, ll):
        ''' This map returns a "regular" list of Oids '''
        if not ll:
            return []
        return [func(i) for i in self.liter(ll)]

    def insertAfter(self, ll, compare, newvalue):
        ''' Inserts a new value @newvalue into a plist @ll after the
        node whose value is equal to @compare. If @compare is not found then an
        exception is raised. '''
        stack = []
        i = ll
        while i:
            found = self.car(i) == compare
            stack.append(i)
            i = self.cdr(i)
            if found:
                break
        if not i:
            # compare not found in ll
            raise RuntimeError("compare value not found in list!")
        newll = self.cons(newvalue, i)
        while stack:
            n = stack.pop()
            newll = self.cons(self.car(n), newll)
        return newll


if __name__ == '__main__':
    import os
    import ostore

    # Global PDS
    global_pstor, oidfs = ostore.init_ostore()
    plistObj = Plist(global_pstor)

    ll = plistObj.plist('Angela', 'Austin', 'Ning', 'Qi')
    print ll
    ll = plistObj.insertAfter(ll, 'Austin', 'Baba')
    res = plistObj.map(lambda n: "*" + n + "*", ll)
    print res

    oidfs.close()
    global_pstor.close()
