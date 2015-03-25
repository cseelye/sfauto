#!/usr/bin/env python 2.7

import argparse
import paramiko
import socket
import sys
import tempfile
import textwrap

def main():
    
    # Connect to the PXE server
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(userArgs.pxeServer, username=userArgs.pxeUsername, password=userArgs.pxePassword)
    except paramiko.AuthenticationException as e:
        print 'Invalid credentials for {}'.format(userArgs.pxeServer)
        sys.exit(1)
    except paramiko.SSHException as e:
        print 'Error connecting to {}: {}'.format(userArgs.pxeServer, e)
        sys.exit(1)
    except (socket.error, socket.timeout) as e:
        print 'Error connecting to {}: {}'.format(userArgs.pxeServer, e)
        sys.exit(1)

    # Delete the PXE config file
    client.exec_command('rm -f /tftpboot/pxelinux.cfg/01-{}'.format(userArgs.macAddress.replace(':', '-')))
    
    print  'Successfully deleted PXE config file from PXE server'
    sys.exit(0)


if __name__ == '__main__':

    # Parse command line
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='Delete PXE imaging file',
                                     epilog=textwrap.dedent('''\
                                                            This script will delete the PXE option file on the PXE server for the given
                                                            MAC address.
                                                            '''))
    parser.add_argument('-m', '--mac-address', type=str, dest='macAddress', required=True, help='The MAC address to remove the PXE config for')
    parser.add_argument('-s', '--pxe-server', type=str, dest='pxeServer', default='192.168.100.4', help='The PXE server to use [%(default)s]')
    parser.add_argument('-u', '--pxe-user', type=str, dest='pxeUsername', default='root', help='The username for the PXE server')
    parser.add_argument('-p', '--pxe-password', type=str, dest='pxePassword', default='SolidF1r3', help='The password for the PXE server')
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    userArgs = parser.parse_args()

    main()

