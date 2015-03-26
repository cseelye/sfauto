#!/usr/bin/env python2.7

"""
This action will create a PXE imaging file

When run as a script, the following options/env variables apply:
    --mac_address       The MAC address of the system to image

    --device_name       The name of the device to use when booting the PXE image [eth0]

    --release           The release branch of the image

    --version           The version of the image

    --pxe_server        The PXE server to use

    --pxe_user          Theusername of the PXE server

    --pxe_pass          The password of the PXE server
"""

import argparse
import paramiko
import socket
import sys
import tempfile
import textwrap
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import lib.libvmware as libvmware

class CreatePxeFileAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({'mac_address' : libsf.isValidMACAddress,
                            'device_name' : None,
                            'release' : None,
                            'version' : None,
                            'pxe_server' : libsf.IsValidIpv4Address,
                            'pxe_user' : None,
                            'pxe_pass' : None},
            args)

    def Execute(self, mac_address, device_name, release, version, pxe_server='192.168.100.4', pxe_user='root', pxe_pass='SolidF1r3', bash=False, csv=False, debug=False):
        """
        Create the PXE config file
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()
        if bash or csv:
            mylog.silence = True

        # Content and location for the PXE boot file
        pxe_file_name = '/tftpboot/pxelinux.cfg/01-{}'.format(mac_address.replace(':', '-'))
        pxe_file_contents = textwrap.dedent(
        '''\
        DEFAULT BootImage
        TIMEOUT 1
        ONTIMEOUT BootImage
        PROMPT 0
        LABEL BootImage
           KERNEL images/fdva/solidfire-fdva-{release}-{version}/casper/vmlinuz
           INITRD images/fdva/solidfire-fdva-{release}-{version}/casper/initrd.lz
           APPEND console=tty0 ip=:::::eth0:dhcp boot=casper vga=791 fetch=ftp://{pxeServer}/images/fdva/solidfire-fdva-{release}-{version}/casper/filesystem.squashfs sf_start_rtfi=1 sf_test_hardware=0 --
        LABEL BootLocal
           localboot 0
        ''').format(release=release, version=version, pxeServer=pxe_server)

        # Connect to the PXE server
        try:
            client = libsf.ConnectSsh(pxe_server, pxe_user, pxe_pass)
        except libsf.SfError as e:
            mylog.error(str(e))
            return False

        # Write the content to a local temp file and upload that file to the PXE server
        with tempfile.NamedTemporaryFile() as temp:
            mylog.info('PXE config file: {}'.format(pxe_file_name))
            mylog.info('PXE file contents:')
            for line in pxe_file_contents.split('\n'):
                mylog.raw(line)
            temp.write(pxe_file_contents)
            temp.flush()
            sftp = client.open_sftp()
            sftp.put(temp.name, pxe_file_name)
            sftp.close()

        client.close()
        mylog.passed('Successfully wrote PXE config file to PXE server')
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='Create PXE imaging file',
                                     epilog=textwrap.dedent('''\
                                                            This script will create the PXE option file on the PXE server for the given
                                                            MAC address.
                                                            To use the defaults (PXE image the default release using the Boulder server)
                                                            you only need to include the version to image to, and the MAC address:
                                                                %(prog)s -v 8.0.0.1234 -m 00:50:56:ab:cd:ef
                                                            To image another branch, include the release option:
                                                                %(prog)s -v 8.0.0.1234 -m 00:50:56:ab:cd:ef -r oxygen-oxygen-mybranch
                                                            If you are not imaging an FDVA/mNode, set the device name appropriately:
                                                                %(prog)s -v 8.0.0.1234 -m 00:50:56:ab:cd:ef -d eth2

                                                            If you are watching the console during RTFI, there are long periods where it
                                                            appears to hang or not be doing anything.  This is normal, just wait for it.
                                                            '''))
    parser.add_argument('-m', '--mac_address', type=str, required=True, help='The MAC address of the system to PXE image')
    parser.add_argument('-e', '--device_name', type=str, default='eth0', help='The device name to use when booting the PXE image [%(default)s]')
    parser.add_argument('-r', '--release', type=str, default='oxygen', help='The release branch of the image [%(default)s]')
    parser.add_argument('-v', '--version', type=str, required=True, help='The version of the image')
    parser.add_argument('-s', '--pxe_server', type=str, default='192.168.100.4', help='The PXE server to use [%(default)s]')
    parser.add_argument('-u', '--pxe_user', type=str, default='root', help='The username for the PXE server')
    parser.add_argument('-p', '--pxe_pass', type=str,  default='SolidF1r3', help='The password for the PXE server')
    parser.add_argument("--csv", action="store_true", default=False, help="Display a minimal output that is formatted as a comma separated list")
    parser.add_argument("--bash", action="store_true", default=False, help="Display a minimal output that is formatted as a space separated list")
    parser.add_argument('--debug', action='store_true', default=False, help='Display more verbose messages')
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    user_args = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(mac_address=user_args.mac_address, device_name=user_args.device_name, release=user_args.release, version=user_args.version, pxe_server=user_args.pxe_server, pxe_user=user_args.pxe_user, pxe_pass=user_args.pxe_pass, bash=user_args.bash, csv=user_args.csv, debug=user_args.debug):
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
