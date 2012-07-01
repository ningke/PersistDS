#!/usr/bin/env python

import os
from pstructstor import PStructStor
from oidfs import OidFS

def init_testpds(testpds_path="/home/ning/local/var/run/testpds"):
    if not os.path.isdir(testpds_path):
        os.mkdir(testpds_path)
    pstor = PStructStor.mkpstor(os.path.join(testpds_path, "pstor"))
    oidfs = OidFS(os.path.join(testpds_path, "oidfs"))
    print "Testpds: initialized %s" % testpds_path
    return (pstor, oidfs)

