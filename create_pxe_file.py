#!/usr/bin/env python 2.7

import argparse
import paramiko
import socket
import sys
import tempfile
import textwrap


def main():
    
    # Content and location for the PXE boot file
    pxeFileName = '/tftpboot/pxelinux.cfg/01-{}'.format(userArgs.macAddress.replace(':', '-'))
    pxeFileContents = '''
    DEFAULT BootImage
    TIMEOUT 1
    ONTIMEOUT BootImage
    PROMPT 0
    LABEL BootImage
       KERNEL images/fdva/solidfire-fdva-{release}-{version}/casper/vmlinuz
       INITRD images/fdva/solidfire-fdva-{release}-{version}/casper/initrd.lz
       APPEND console=tty0 ip=:::::eth0:dhcp boot=casper vga=791 fetch=ftp://{pxeServer}/images/fdva/solidfire-fdva-{release}-{version}/casper/filesystem.squashfs sf_start_rtfi=1 sf_test_hardware=1 --
    LABEL BootLocal
       localboot 0
    '''.format(release=userArgs.release, version=userArgs.version, pxeServer=userArgs.pxeServer)
    
    
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
    
    # Write the content to a local temp file and upload that file to the PXE server
    with tempfile.NamedTemporaryFile() as temp:
        print 'PXE config file: {}'.format(pxeFileName)
        print 'PXE file contents: {}'.format(pxeFileContents)
        temp.write(pxeFileContents)
        temp.flush()
        sftp = client.open_sftp()
        sftp.put(temp.name, pxeFileName)
        sftp.close()
        client.close()
    
    print  'Successfully wrote PXE config file to PXE server'
    sys.exit(0)


if __name__ == '__main__':

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
                                                            '''))
    parser.add_argument('-m', '--mac-address', type=str, dest='macAddress', required=True, help='The MAC address of the system to PXE image')
    parser.add_argument('-d', '--device-name', type=str, dest='deviceName', default='eth0', help='The device name to use when booting the PXE image [%(default)s]')
    parser.add_argument('-r', '--release', type=str, default='oxygen', help='The release branch of the image [%(default)s]')
    parser.add_argument('-v', '--version', type=str, required=True, help='The version of the image')
    parser.add_argument('-s', '--pxe-server', type=str, dest='pxeServer', default='192.168.100.4', help='The PXE server to use [%(default)s]')
    parser.add_argument('-u', '--pxe-user', type=str, dest='pxeUsername', default='root', help='The username for the PXE server')
    parser.add_argument('-p', '--pxe-password', type=str, dest='pxePassword', default='SolidF1r3', help='The password for the PXE server')
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    userArgs = parser.parse_args()

    main()

