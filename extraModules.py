import os
import subprocess
import sys
import tempfile
from pyhs2 import *
import MySQLdb
import sasl


def run_bash(comm):
    params = comm.split(' ')
    subprocess.call(params)


def create_dir(direc):
    direc = os.path.expanduser(direc)
    if not os.path.exists(direc):
        os.makedirs(direc)


# Multiline bash
def run_script(script):
    with tempfile.NamedTemporaryFile() as scriptfile:
        scriptfile.write(script)
        scriptfile.flush()
        subprocess.call(['/bin/bash', scriptfile.name])


def getHiveConn(my_host, my_port, my_authMechanism, my_user, my_pass):
    # Currently using no pass
    conn = connect(host=my_host, port=my_port, authMechanism=my_authMechanism, user=my_user)
    cursor = conn.cursor()
    return cursor


def dir_create(dirarr):
    for destination in dirarr:
        create_dir(destination)

