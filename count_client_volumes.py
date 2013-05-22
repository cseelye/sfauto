#!/usr/bin/python

"""
This action will count all of the connected iSCSI volumes and compare to the exepected value

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients
    SFCLIENT_IPS env var

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --expected          The expected number of volumes
"""

import sys
from optparse import OptionParser
import re
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CountClientVolumesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"client_ips" : libsf.IsValidIpv4AddressList,
                            "expected" : libsf.IsInteger},
            args)
        if args["exepected"] < 0:
            raise libsf.SfArgumentError("Invalid value for expected")

    def Execute(self, expected, client_ips=None, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_user, debug=False):
        """
        Count iSCSI volumes connected to clients
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        allgood = True
        for client_ip in client_ips:
            try:
                # Connect to client
                ssh = libsf.ConnectSsh(client_ip, client_user, client_pass)

                # Get the hostname of the client
                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "hostname")
                client_hostname = stdout.readlines()[0].strip()

                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "iscsiadm -m session -P 3 | egrep \"Target|Current Portal|iSCSI Session State|Attached scsi\"")
                data = stdout.readlines()
                new_volume = None
                volumes = dict()
                for line in data:
                    m = re.search(r"Target:\s+(.+)", line)
                    if(m):
                        new_volume = dict()
                        new_volume["iqn"] = m.group(1)

                    m = re.search(r"Current Portal:\s+(.+):", line)
                    if(m):
                        new_volume["portal"] = m.group(1)

                    m = re.search(r"Session State:\s+(.+)", line)
                    if(m):
                        new_volume["state"] = m.group(1)

                    m = re.search(r"disk\s+(\S+)\s", line)
                    if(m):
                        new_volume["device"] = m.group(1)
                        # we don't add it to the dict until we find a valid device
                        volumes[m.group(1)] = new_volume

                volume_count = len(volumes)
                if (volume_count == expected):
                    mylog.passed("Found " + str(volume_count) + " iSCSI volumes on client '" + client_hostname + "'")
                else:
                    mylog.error("Expected " + str(expected) + " but found " + str(volume_count) + " iSCSI volumes on client '" + client_hostname + "'")
                    allgood = False
            except libsf.SfError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                allgood = False
                continue

        if allgood:
            mylog.passed("Found expected number of volumes on all clients")
        else:
            mylog.error("Did not find expected number of volumes on all clients")
        return allgood

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-c", "--client_ips", type="string", dest="client_ips", default=None, help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("--expected", type="int", dest="expected", default=None, help="the number of drives to expect")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.expected, options.client_ips, options.client_user, options.client_pass, options.debug):
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
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
