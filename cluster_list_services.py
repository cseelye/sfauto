#!/usr/bin/env python2.7

"""
This action will display a list of the requested type of services in the cluster
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, SelectionType, OptionalValueType, StrType
from libsf import sfdefaults
from libsf import SolidFireError
import sys
import json

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "service_type" : (StrType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "output_format" : (OptionalValueType(SelectionType(sfdefaults.all_output_formats)), None),
})
def ClusterListServices(service_type,
                        mvip,
                        username,
                        password,
                        output_format):
    """
    Get the list of matching services

    Args:
        service_type:   the type of service to display
        output_format:  the output format to use; if specified logging will be silenced and the requested minimal format used
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    try:
        service_list = SFCluster(mvip, username, password).ListServices()
    except SolidFireError as e:
        log.error("Could not search for services: {}".format(e))
        return False

    services = sorted([(item["service"]["serviceID"], item["service"]["serviceType"]) for item in service_list if "service" in item])

    if service_type:
        services = [item[0] for item in services if item[1] == service_type]
    else:
        services = ["{}{}".format(item[1], item[0]) for item in services]

    # Display the list in the requested format
    if output_format and output_format == "bash":
        sys.stdout.write(" ".join([str(item) for item in services]) + "\n")
        sys.stdout.flush()
    elif output_format and output_format == "json":
        sys.stdout.write(json.dumps({"services" : services}) + "\n")
        sys.stdout.flush()
    else:
        for service_id, service_type in services:
            log.info("  {} {}".format(service_type, service_id))

    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--type", dest="service_type", help="the type of service to show")
    parser.add_console_format_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterListServices, args)
    app.Run(**args)
