#!/usr/bin/env python2.7

"""
This action will show the volumes connected to a client
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfclient import SFClient
from libsf.util import ValidateAndDefault, IPv4AddressType, SelectionType, OptionalValueType, StrType
from libsf import sfdefaults
from libsf import SolidFireError
import json
import sys

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "client_ip" : (IPv4AddressType, sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
    "sort" : (StrType, "iqn"),
    "output_format" : (OptionalValueType(SelectionType(sfdefaults.all_output_formats)), None),
})
def GetClientVolumes(client_ip,
                     client_user,
                     client_pass,
                     sort,
                     output_format):
    """
    Show the volumes connected to a client

    Args:
        client_ips:         the list of client IP addresses
        client_user:        the username for the clients
        client_pass:        the password for the clients
        output_format:     the output format to use; if specified logging will be silenced and the requested minimal format used
    """
    log = GetLogger()

    try:
        log.info("Connecting...")
        client = SFClient(client_ip, client_user, client_pass)
        log.info("Gathering data about connected volumes...")
        volumes = client.GetVolumeSummary()
    except SolidFireError as ex:
        log.error(ex)
        return False

    if output_format and output_format == "bash":
        sys.stdout.write(" ".join([volume["iqn"] for volume in volumes.itervalues()]) + "\n")
        sys.stdout.flush()
    elif output_format and output_format == "json":
        sys.stdout.write(json.dumps({"volumes" : volumes}) + "\n")
        sys.stdout.flush()
    else:
        log.info("Found {} iSCSI volumes on {}".format(len(volumes.keys()), client.hostname))
        for _, volume in sorted(volumes.iteritems(), key=lambda (k, v): v[sort]):
            for key in ["sid", "state"]:
                if key not in volume.keys():
                    volume[key] = "unknown"
            log.info("   {} -> {}, SID: {}, SectorSize: {}, Portal: {}, State: {}".format(volume["iqn"], volume["device"], volume["sid"], volume["sectors"], volume["portal"], volume["state"]))

    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_single_client_args()
    parser.add_argument("--sort", type=str, choices=sfdefaults.all_client_volume_sort, default=sfdefaults.client_volume_sort, help="the sort order to display the volumes")
    parser.add_console_format_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(GetClientVolumes, args)
    app.Run(**args)
