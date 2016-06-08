#!/usr/bin/env python2.7
"""
Helpers for PXE booting
"""
import tempfile
from .logutil import GetLogger
from . import sfdefaults
from . import SSHConnection

log = GetLogger()

PXE_CONFIG_PATH = "/tftpboot/pxelinux.cfg/01-{}"

def CreatePXEFile(macAddress,
                  repo,
                  version,
                  additionalOptions="",
                  baseOptions=sfdefaults.rtfi_options,
                  imageType="rtfi",
                  pxeServer=sfdefaults.pxe_server,
                  pxeUser=sfdefaults.pxe_username,
                  pxePassword=sfdefaults.pxe_password,
                  bootNic="eth2",
                  ip=None,
                  netmask=None,
                  gateway=None,
                  hostname=None,
                  ):
    """
    Create a config file for a machine on the PXE server

    Args:
        macAddress:         the MAC address of the machine that will be PXE booted
        repo:               the repository the image is from
        version:            the version string of the image
        additionalOptions:  additional RTFI options to add to the defaults
        baseOptions:        the default RTFI options
        imageType:          type of image (rtfi or fdva)
        pxeServer:          the server to put the config file on
        pxeUser:            the username of the server
        pxePassword:        the password of the server
        bootNic:            the NIC the machine to be imaged will boot from
        
    """
    options = baseOptions
    if additionalOptions:
        options = "{},{}".format(baseOptions, additionalOptions)
    options = options.replace(",", " ")

    transformed_mac = macAddress.lower().replace(":", "-")

    # IP configuration params documented here:
    # https://www.kernel.org/doc/Documentation/filesystems/nfs/nfsroot.txt
    autoconf="off" if ip else "dhcp"
    ip = ip or ""
    netmask = netmask or ""
    gateway = gateway or ""
    hostname = hostname or ""

    pxe_file_contents = \
"""DEFAULT BootImage
TIMEOUT 3
ONTIMEOUT BootImage
PROMPT 0
LABEL BootImage
    KERNEL images/{imageType}/solidfire-{imageType}-{repo}-{version}/casper/vmlinuz
    INITRD images/{imageType}/solidfire-{imageType}-{repo}-{version}/casper/initrd.lz
    APPEND console=tty0 ip={ip}::{gateway}:{netmask}:{hostname}:{bootNic}:{autoconf} boot=casper vga=791 console=tty0 console=ttyS1,115200n8 fetch=ftp://{pxeServer}/images/{imageType}/solidfire-{imageType}-{repo}-{version}/casper/filesystem.squashfs {options} --
LABEL BootLocal
    localboot 0
""".format(imageType=imageType,
           repo=repo,
           version=version,
           bootNic=bootNic,
           pxeServer=pxeServer,
           options=options,
           autoconf=autoconf,
           ip=ip,
           netmask=netmask,
           gateway=gateway,
           hostname=hostname)

    log.debug("Sending PXE config file {} with contents:\n{}".format(transformed_mac, pxe_file_contents))
    with tempfile.NamedTemporaryFile() as temp:
        temp.write(pxe_file_contents)
        temp.flush()

        with SSHConnection(pxeServer, pxeUser, pxePassword) as ssh:
            ssh.PutFile(temp.name, PXE_CONFIG_PATH.format(transformed_mac))

def DeletePXEFile(macAddress, pxeServer=sfdefaults.pxe_server, pxeUser=sfdefaults.pxe_username, pxePassword=sfdefaults.pxe_password):
    """
    Remove a PXE config file from the PXE server
    """
    transformed_mac = macAddress.lower().replace(":", "-")
    with SSHConnection(pxeServer, pxeUser, pxePassword) as ssh:
        ssh.RunCommand("rm -f " + PXE_CONFIG_PATH.format(transformed_mac))

