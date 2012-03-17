import sys
import os
import testpds
import ptrie
from ptrie import Ptrie
from oidfs import OidFS

class PtrieTester(object):
    def __init__(self):
        # Get a PStructStor
        self.pstor = testpds.getPStor()
        self.ptrieObj = Ptrie(self.pstor)
        self.root = ptrie.Nulltrie # Start with Nulltrie as root
        self.oidfs = OidFS("/home/ning/Dropbox/run/testoidfs")

    def run(self):
        # start test loop
        self.testloop()
        self.pstor.close()

    # Find words
    def findWords(self, words):
        for w in words:
            wnode = self.ptrieObj.find(self.root, w)
            if not wnode:
                print "Error: Cannot find %s" % w
            else:
                print "Found %s at %s" % (w, wnode)

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
            print "(", f['prefix'], ")"

    def makeBFSPrintFunc(self, initialLevel=-1):
        env = {'level': initialLevel}
        def prnode(tn):
            fields = self.ptrieObj.getfields(tn)
            prefix = fields['prefix']
            if fields['final']:
                pfxString = "( " + prefix + " )"
            else:
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
            inputtext = raw_input(">> ");
            args = inputtext.split()
            if len(args) == 0:
                cmd = "help"
            else:
                cmd = args.pop(0)

            if cmd == "help":
                print """Type a command. Commands are:
help quit read load find delete insert dfwalk bfwalk gc save"""
            elif cmd == "quit":
                ans = raw_input("Save? (y/n)")
                if ans == "y":
                    self.oidfs.save(self.root, "examplePtrie")
                    print "Ptrie saved as \"examplePtrie\""
                break
            elif cmd == "read":
                # Insert words from a list
                fobj = open(args[0])
                words = fobj.read()
                words = eval(words) # words must be in the format of a python list
                fobj.close()
                print words
                # Insert words into trie
                for w in words:
                    self.root = self.ptrieObj.insert(self.root, w)
            elif cmd == "load":
                # Load a previously saved ptrie
                oidname = args[0]
                self.root = self.oidfs.find(oidname)
            elif cmd == "find":
                self.findWords(args)
            elif cmd == "delete":
                self.delete(args[0])
            elif cmd == "insert":
                self.root = self.ptrieObj.insert(self.root, args[0])
            elif cmd == "dfwalk":
                print "Depth-First Walk:"
                self.ptrieObj.dfSearch(self.root, self.printNode)
            elif cmd == "bfwalk":
                print "Breadth-First Walk:"
                self.bfwalk()
            elif cmd == "gc":
                # Do a garbage collection
                print "Garbage Collecting..."
                node_sho = self.ptrieObj.find(self.root, "sho", False)
                self.root, node_sho = self.pstor.keepOids([self.root, node_sho])
            elif cmd == "save":
                # Save the root in oidfs
                if len(args):
                    oidname = args[0]
                else:
                    oidname = "examplePtrie"
                self.oidfs.save(self.root, oidname)
            else:
                print "Bad command: type 'quit' to quit"


tester = PtrieTester()
tester.run()
