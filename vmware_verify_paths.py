#!/usr/bin/env python

"""
This action will verify the correct count and health of volumes/paths

When run as a script, the following options/env variables apply:
    --mgmt_server       The IP/hostname of the vSphere Server

    --mgmt_user         The vsphere admin username

    --mgmt_pass         The vsphere admin password

    --vmhost            The IP of the ESX host to verify paths on

    --exected_volumes   The expected number of volumes

    --expected_paths    The expected number of paths per volume

"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
import lib.libvmware as libvmware

class VmwareVerifyPathsAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mgmt_server" : libsf.IsValidIpv4Address,
                            "mgmt_user" : None,
                            "mgmt_pass" : None,
                            "vmhost" : libsf.IsValidIpv4AddressList,
                            "expected_volumes" : None,
                            "expected_paths" : None},
            args)

    def Execute(self, vmhost, expected_volumes, expected_paths=4, mgmt_server=sfdefaults.fc_mgmt_server, mgmt_user=sfdefaults.fc_vsphere_user, mgmt_pass=sfdefaults.fc_vsphere_pass, debug=False):
        """
        Set the multipath policy
        """
        if vmhost == None:
            vmhost = []
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Connecting to vSphere " + mgmt_server)
        try:
            with libvmware.VsphereConnection(mgmt_server, mgmt_user, mgmt_pass) as vsphere:

                allgood = True
                for host_ip in vmhost:
                    # Find the requested host
                    mylog.info("Searching for host " + host_ip)
                    host = libvmware.FindHost(vsphere, host_ip)

                    mylog.info("Checking connected volumes")
                    lun2multipath = {}
                    if not host.config.storageDevice.multipathInfo.lun:
                        mylog.error("No multipath LUNs detected")
                        allgood = False
                    else:
                        for mp in host.config.storageDevice.multipathInfo.lun:
                            lun2multipath[mp.lun] = mp

                    volume_count = 0
                    total_paths = 0
                    total_unhealthy_paths = 0
                    total_unhealthy_volumes = 0
                    for lun in host.config.storageDevice.scsiLun:
                        # skip non-SF devices
                        if lun.vendor != "SolidFir":
                            continue

                        volume_count += 1
                        key = lun.key
                        mp = lun2multipath[key]
                        pieces = lun.canonicalName.split('.')
                        disk_serial = pieces[-1]
                        volume_id = int(disk_serial[24:32].strip('0'), 16)

                        volume_paths = len(mp.path)
                        total_paths += volume_paths
                        #if volume_paths < expected_paths:
                        #    mylog.error("Volume {} (volumeID {}) only has {} paths but expected {}".format(lun.canonicalName, volume_id, volume_paths, expected_paths))
                        #    allgood = False

                        unhealthy_paths = 0
                        for path in mp.path:
                            if path.state != "active":
                                unhealthy_paths += 1
                        total_unhealthy_paths += unhealthy_paths
                        if volume_paths - unhealthy_paths < expected_paths:
                            mylog.error("Volume {} (volumeID {}) only has {} healthy paths but expected {}".format(lun.canonicalName, volume_id, volume_paths - unhealthy_paths, expected_paths))
                            allgood = False

                        for message in lun.operationalState:
                            if "error" in message.lower():
                                mylog.error("Volume {} (volumeID {}) is in an error state - ".format(lun.canonicalName, volume_id))
                                mylog.error("\n".join(lun.operationalState))
                                total_unhealthy_volumes += 1
                                allgood = False

        except libvmware.VmwareError as e:
            mylog.error(str(e))
            return False

        if volume_count < expected_volumes:
            mylog.error("Found {} volumes but expected {}".format(volume_count, expected_volumes))
        else:
            mylog.passed("Found {} volumes".format(volume_count))

        if expected_volumes * expected_paths > total_paths:
            mylog.error("Found {} but expected {} total paths".format(total_paths, expected_volumes * expected_paths))
        else:
            mylog.passed("Found {} total paths".format(total_paths))

        if total_unhealthy_paths > 0:
            mylog.error("Found {} unhealthy paths".format(unhealthy_paths))

        if total_unhealthy_volumes > 0:
            mylog.error("Found {} unhealthy volumes".format(total_unhealthy_volumes))

        if allgood:
            return True
        else:
            return False


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-s", "--mgmt_server", type="string", dest="mgmt_server", default=sfdefaults.fc_mgmt_server, help="the IP/hostname of the vSphere Server [%default]")
    parser.add_option("-m", "--mgmt_user", type="string", dest="mgmt_user", default=sfdefaults.fc_vsphere_user, help="the vsphere admin username [%default]")
    parser.add_option("-a", "--mgmt_pass", type="string", dest="mgmt_pass", default=sfdefaults.fc_vsphere_pass, help="the vsphere admin password [%default]")
    parser.add_option("-o", "--vmhost", action="list", dest="vmhost", default=None, help="the IP of one or more ESX hosts to verify on")
    parser.add_option("--expected_volumes", type="int", dest="expected_volumes", default=4, help="the expected number of volumes")
    parser.add_option("--expected_paths", type="int", dest="expected_paths", default=4, help="the expected number of paths per volume")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vmhost=options.vmhost, expected_volumes=options.expected_volumes, expected_paths=options.expected_paths, mgmt_server=options.mgmt_server, mgmt_user=options.mgmt_user, mgmt_pass=options.mgmt_pass, debug=options.debug):
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
