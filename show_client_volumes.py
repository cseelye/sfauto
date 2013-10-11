#!/usr/bin/python

"""
This action will show the connected iSCSI volumes on a client

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients
    SFCLIENT_IPS env var

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --sort              Sort order to display the volumes in [iqn, device, portal, state]
"""


from optparse import OptionParser
import sys
import logging
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import SfClient, ClientError, OsType
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class ShowClientVolumesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_CLIENT = "BEFORE_CLIENT"
        AFTER_CLIENT = "AFTER_CLIENT"
        CLIENT_FAILED = "CLIENT_FAILED"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientIPs" : libsf.IsValidIpv4AddressList},
            args)

    def Execute(self, clientIPs=None, sort=sfdefaults.client_volume_sort, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, debug=False):
        """
        Show the iSCSI volumes connected to a client
        """
        if not clientIPs:
            clientIPs = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        if sort not in sfdefaults.all_client_volume_sort:
            sort = "iqn"

        for client_ip in clientIPs:
            self._RaiseEvent(self.Events.BEFORE_CLIENT, clientIP=client_ip)
            client = SfClient()
            mylog.info(client_ip + ": Connecting to client")
            try:
                client.Connect(client_ip, clientUser, clientPass)
            except ClientError as e:
                mylog.error(client_ip + ": " + e.message)
                self._RaiseEvent(self.Events.CLIENT_FAILED, clientIP=client_ip, exception=e)
                continue

            mylog.info(client_ip + ": Gathering information about connected volumes...")
            try:
                volumes = client.GetVolumeSummary()
            except ClientError as e:
                mylog.error(client_ip + ": " + e.message)
                self._RaiseEvent(self.Events.CLIENT_FAILED, clientIP=client_ip, exception=e)
                continue

            mylog.info(client_ip + ": Found " + str(len(volumes.keys())) + " iSCSI volumes on " + client.Hostname + ":")
            for device, volume in sorted(volumes.iteritems(), key=lambda (k, v): v[sort]):
                if "sid" not in volume.keys():
                    volume["sid"] = "unknown"
                outstr = "   " + volume["iqn"] + " -> " + volume["device"] + ", SID: " + volume["sid"] + ", SectorSize: " + volume["sectors"] + ", Portal: " + volume["portal"]
                if "state" in volume:
                    outstr += ", Session: " + volume["state"]
                mylog.info(outstr)
            self._RaiseEvent(self.Events.AFTER_CLIENT, clientIP=client_ip)

            return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-c", "--client_ips", action="list", dest="client_ips", default=None, help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("--sort", type="choice", choices=sfdefaults.all_client_volume_sort, dest="sort", default=sfdefaults.client_volume_sort, help="the sort order to display the volumes [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ips, options.sort, options.client_user, options.client_pass, options.debug):
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

