#!/usr/bin/env python

import os
from pstructstor import PStructStor

def getPStor(testpds_path="/home/ning/Dropbox/run/testpds/fixszPDS001"):
    if not os.path.isdir(testpds_path):
        os.mkdir(testpds_path)
    pstor = PStructStor(testpds_path)
    print pstor
    return pstor

