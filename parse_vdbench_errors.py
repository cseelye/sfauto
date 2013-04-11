#!/usr/bin/python

# This script will parse io errors from vdbench output

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

output_dir = "output"               # The directory containing the vdbench output files
                                    # --output_dir

client_user = "root"                # The username for the client
                                    # --client_user

client_pass = "password"           # The password for the client
                                    # --client_pass


# ----------------------------------------------------------------------------


import sys,os
from optparse import OptionParser
import re
import commands
import libsf
from libsf import mylog
import libclient
from libclient import SfClient, ClientError

def main():
    global output_dir, client_user, client_pass

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "client_user", "client_pass" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--output_dir", type="string", dest="output_dir", default=output_dir, help="the directory containing the vdbench output files [%default]")
    parser.add_option("--client_user", type="string", dest="client_user", default=client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=client_pass, help="the password for the clients [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    client_user = options.client_user
    client_pass = options.client_pass
    output_dir = options.output_dir
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)

    mylog.info("Reading logfile.html")
    raw_error_list = []
    try:
        with open(output_dir + "/logfile.html") as f:
            line = None
            while line != "":
                line = f.readline()
                if "EIO" in line: raw_error_list.append(line.strip())
    except EnvironmentError:
        mylog.error("Could not read logfile.html")
        sys.exit(1)
    if len(raw_error_list) <= 0:
        mylog.info("No errors found")
        sys.exit(0)
    
    mylog.info("Reading parmfile.html")
    raw_system_list = []
    try:
        with open(output_dir + "/parmfile.html") as f:
            line = None
            while line != "":
                line = f.readline()
                if "system" in line: raw_system_list.append(line.strip())
    except EnvironmentError:
        mylog.error("Could not read parmfile.html")
        sys.exit(1)
    
    # Make a hash of hd to system
    hd2system = dict()
    for line in raw_system_list:
        hd = None
        s = None
        pieces = line.split(',')
        for piece in pieces:
            if piece.startswith("hd="):
                hd = piece[3:]
            if piece.startswith("system="):
                s = piece[7:]
        if hd and s:
            hd2system[hd] = s
    
    # Get a mapping of volumes to luns on each affected system
    lun2volume = dict()
    for line in raw_error_list:
        hg = None
        system = None
        pieces = re.split("\s+", line)
        hd_piece = pieces[1]
        if "localhost" in hd_piece:
            system = "localhost"
        else:
            m = re.search("(hd\d+)-", hd_piece)
            if m:
                hd = m.group(1)
                system = hd2system[hd]
        
        if system not in lun2volume.keys():
            mylog.info("Getting a list of volumes on " + system)
            client = SfClient()
            try:
                client.Connect(system, client_user, client_pass)
            except ClientError as e:
                mylog.warning("Could not connect to " + system + ": " + str(e))
                continue
            try:
                lun2volume[system] = client.GetVolumeSummary()
            except ClientError as e:
                mylog.warning("Could not get volume list from " + system + ": " + str(e))
                continue
    
    mylog.info("Client mapping:")
    for hd in sorted(hd2system.keys()):
        system = hd2system[hd]
        mylog.raw(str(hd) + " => " + str(system))
    
    mylog.error("IO errors detected:")
    for line in raw_error_list:
        pieces = re.split("\s+", line)
        hd_piece = pieces[1]
        error_time = pieces[2]
        io_type = pieces[4]
        lun = pieces[6]
        lba = pieces[9]
        io_size = pieces[11]
        
        system = None
        if "localhost" in hd_piece:
            system = "localhost"
        else:
            m = re.search("(hd\d+)-", hd_piece)
            if m:
                hd = m.group(1)
                system = hd2system[hd]
        volume = None
        if system in lun2volume.keys() and lun in lun2volume[system].keys():
            volume = lun2volume[system][lun]
        
        mylog.raw(error_time + "  " + io_type.upper() + " ERROR on " + system + " " + lun + " LBA " + lba + " xfer " + io_size)
        mylog.raw("              " + volume["iqn"])
        mylog.raw("              " + str(volume["sectors"]) + " sector size currently connected to " + volume["portal"])
    
    sys.exit(1)

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        #timer = libsf.ScriptTimer()
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)






