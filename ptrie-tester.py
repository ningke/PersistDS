#!/usr/bin/env python

import sys
import os
import testpds
import ptrie
from ptrie import Ptrie
from oidfs import OidFS

class PtrieTester(object):
    def __init__(self, pdspath="/home/ning/local/var/run/testpds"):
        # Get a PStructStor and Oidfs
        self._reinit(pdspath)

    def _reinit(self, pdspath):
        self.pstor, self.ofs = testpds.init_testpds(pdspath)
        self.ptrieObj = Ptrie(self.pstor)
        self.root = ptrie.Nulltrie # Start with Nulltrie as root

    def run(self):
        # start test loop
        self.testloop()
        self.pstor.close()

    # Find words
    def findWords(self, words):
        for w in words:
            wnode = self.ptrieObj.find(self.root, w)
            if not wnode:
                print "Cannot find %s" % w
            else:
                fields = self.ptrieObj.getfields(wnode)
                print "Found %s (%d) at %s" % (w, fields['value'], wnode)

    def delete(self, word):
        newroot = self.ptrieObj.delete(self.root, word)
        if newroot is self.root:
            print "%s is not found, nothing deleted!"
        else:
            print "%s is deleted from ptrie" % word
            self.root = newroot

    def printNode(self, node):
        f = self.ptrieObj.getfields(node)
        if f['final']:
            print "(%s : %s)" % (f['prefix'], f['value'])

    def makeBFSPrintFunc(self, initialLevel=-1):
        env = {'level': initialLevel}
        def prnode(tn):
            fields = self.ptrieObj.getfields(tn)
            prefix = fields['prefix']
            if fields['final']:
                pfxString = "(%s : %s)" % (fields['prefix'], fields['value'])
            else:
                assert(fields['value'] is None)
                pfxString = prefix
            if len(prefix) != env['level']:
                env['level'] += 1
                print "level %d:" % env['level']
            print pfxString
        return prnode

    def bfwalk(self):
        self.ptrieObj.bfsearch(self.root, self.makeBFSPrintFunc(-1))

    def testloop(self):
        while True:
            try:
                inputtext = raw_input(">> ");
            except EOFError as e:
                print "Goodbye!"
                self.ofs.gc()
                self.ofs.close()
                self.pstor.close()
                return
            args = inputtext.split()
            if len(args) == 0:
                cmd = "help"
            else:
                cmd = args.pop(0)

            if cmd == "help":
                print """Type a command. Commands are:
help quit read load find delete insert dfwalk bfwalk gc save ls"""
            elif cmd == "quit":
                ans = raw_input("Save? (y/n)")
                if ans == "y":
                    self.ofs.store(self.root, "examplePtrie")
                    self.ofs.close()
                    self.pstor.close()
                    print "Ptrie saved as \"examplePtrie\""
                break
            elif cmd == "print":
                try:
                    print eval(args[0])
                except Exception as e:
                    print e
            elif cmd == "reinit":
                pdspath = args[0]
                if os.path.exists(pdspath) and os.path.isabs(pdspath):
                    self._reinit(pdspath)
                else:
                    print "Must be an existing absolute path!"
            elif cmd == "read":
                # Insert words from a list
                fobj = open(args[0])
                words = fobj.read()
                words = eval(words) # words must be in the format of a python list
                fobj.close()
                print words
                # Insert words into trie
                for w in words:
                    self.root = self.ptrieObj.insert(
                        self.root, w, 1, lambda v1, v2: v1 + v2)
            elif cmd == "load":
                # Load a previously saved ptrie
                oidname = args[0]
                self.root = self.ofs.load(oidname)
                if not self.root:
                    print "Not found: %s" % oidname
            elif cmd == "find":
                self.findWords(args)
            elif cmd == "delete":
                self.delete(args[0])
            elif cmd == "insert":
                if len(args) == 1:
                    self.root = self.ptrieObj.insert(
                        self.root, args[0], 1, lambda v1, v2: v1 + v2)
                elif len(args) == 2:
                    self.root = self.ptrieObj.insert(
                        self.root, args[0], args[1], lambda v1, v2: v2)
            elif cmd == "dfwalk":
                print "Depth-First Walk:"
                self.ptrieObj.dfSearch(self.root, self.printNode)
            elif cmd == "bfwalk":
                print "Breadth-First Walk:"
                self.bfwalk()
            elif cmd == "gc":
                # Do a garbage collection
                print "Garbage Collecting pstor"
                print type(self.root)
                self.root, = self.pstor.keepOids([self.root])
                print "Garbage Collecting oidfs"
                self.ofs.gc()
            elif cmd == "save":
                # Save the root in oidfs
                if len(args):
                    oidname = args[0]
                else:
                    oidname = "examplePtrie"
                self.ofs.store(self.root, oidname)
            elif cmd == "ls":
                self.ofs.lsoid()
            elif cmd == "remove-file":
                if len(args):
                    oidname = args[0]
                else:
                    oidname = "examplePtrie"
                self.ofs.delete(oidname)
            else:
                print "Bad command: type 'quit' to quit"


tester = PtrieTester()
tester.run()
