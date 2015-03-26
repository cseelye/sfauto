#!/usr/bin/env python2.7

"""
This action will delete a PXE imaging file

When run as a script, the following options/env variables apply:
    --mac_address       The MAC address of the system to image

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

class DeletePxeFileAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({'mac_address' : libsf.isValidMACAddress,
                            'pxe_server' : libsf.IsValidIpv4Address,
                            'pxe_user' : None,
                            'pxe_pass' : None},
            args)

    def Execute(self, mac_address, pxe_server='192.168.100.4', pxe_user='root', pxe_pass='SolidF1r3', bash=False, csv=False, debug=False):
        """
        Delete the PXE config file
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()
        if bash or csv:
            mylog.silence = True


        # Connect to the PXE server
        try:
            client = libsf.ConnectSsh(pxe_server, pxe_user, pxe_pass)
        except libsf.SfError as e:
            mylog.error(str(e))
            return False

        # Remove the config file
        pxe_file_name = '/tftpboot/pxelinux.cfg/01-{}'.format(mac_address.replace(':', '-'))
        libsf.ExecSshCommand(client, 'rm -f {}'.format(pxe_file_name))

        client.close()
        mylog.passed('Successfully removed PXE config file from PXE server')
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='Delete PXE imaging file',
                                     epilog=textwrap.dedent('''\
                                                            This script will remove the PXE option file on the PXE server for the given
                                                            MAC address.
                                                            To use the defaults (the Boulder server) you only need to include the
                                                            MAC address:
                                                                %(prog)s -m 00:50:56:ab:cd:ef
                                                            '''))
    parser.add_argument('-m', '--mac_address', type=str, required=True, help='The MAC address of the system to PXE image')
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
        if Execute(mac_address=user_args.mac_address, pxe_server=user_args.pxe_server, pxe_user=user_args.pxe_user, pxe_pass=user_args.pxe_pass, bash=user_args.bash, csv=user_args.csv, debug=user_args.debug):
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
