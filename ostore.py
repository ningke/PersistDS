import os
from pstructstor import PStructStor
from oidfs import OidFS

##
# ostore: oid (object) store. This module combines the creation of a
# pstructstor and a oidfs. The oidfs thus created only manages OIDs in
# the same pstor.
#
def init_ostore(ostore_path="/home/ning/local/run/test_ostore"):
    if not os.path.isdir(ostore_path):
        os.mkdir(ostore_path)
    pstor = PStructStor.mkpstor(os.path.join(ostore_path, "pstor"))
    oidfs = OidFS(os.path.join(ostore_path, "oidfs"))
    print "OStore: initialized %s" % ostore_path
    return (pstor, oidfs)

