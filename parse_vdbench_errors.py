#!/usr/bin/python

"""
This action will parse vdbench output and show more details about IO errors

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var
"""

import sys
from optparse import OptionParser
import re
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import SfClient, ClientError
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class ParseVdbenchErrorsAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"client_ips" : libsf.IsValidIpv4AddressList,
                            },
            args)

    def Execute(self, output_dir, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Parse vdbench output files for errors
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Reading logfile.html")
        raw_error_list = []
        try:
            with open(output_dir + "/logfile.html") as f:
                line = None
                while line != "":
                    line = f.readline()
                    if "EIO" in line:
                        raw_error_list.append(line.strip())
        except EnvironmentError:
            mylog.error("Could not read logfile.html")
            sys.exit(1)
        if len(raw_error_list) <= 0:
            mylog.info("No errors found")
            return True

        mylog.info("Reading parmfile.html")
        raw_system_list = []
        try:
            with open(output_dir + "/parmfile.html") as f:
                line = None
                while line != "":
                    line = f.readline()
                    if "system" in line:
                        raw_system_list.append(line.strip())
        except EnvironmentError:
            mylog.error("Could not read parmfile.html")
            return False

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
            hd = None
            system = None
            pieces = re.split(r"\s+", line)
            hd_piece = pieces[1]
            if "localhost" in hd_piece:
                system = "localhost"
            else:
                m = re.search(r"(hd\d+)-", hd_piece)
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
            pieces = re.split(r"\s+", line)
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
                m = re.search(r"(hd\d+)-", hd_piece)
                if m:
                    hd = m.group(1)
                    system = hd2system[hd]
            volume = None
            if system in lun2volume.keys() and lun in lun2volume[system].keys():
                volume = lun2volume[system][lun]

            mylog.raw(error_time + "  " + io_type.upper() + " ERROR on " + system + " " + lun + " LBA " + lba + " xfer " + io_size)
            mylog.raw("              " + volume["iqn"])
            mylog.raw("              " + str(volume["sectors"]) + " sector size currently connected to " + volume["portal"])

        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--output_dir", type="string", dest="output_dir", default=None, help="the directory containing the vdbench output files [%default]")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.output_dir, options.client_user, options.client_pass, options.debug):
            sys.exit(0)
        else:
            sys.exit(1)
    except libsf.SfArgumentError as e:
        mylog.error("Invalid arguments - \n" + str(e))
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        Abort()
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

