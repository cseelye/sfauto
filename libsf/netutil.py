#!/usr/bin/env python2.7
"""
Helpers for network related
"""

import ctypes
import email.encoders
from email.mime.multipart import MIMEMultipart, MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import os
import platform
import smtplib
import socket
import struct
from . import shellutil

LOCAL_SYS = platform.system()

def Ping(address):
    """
    Ping a host
    
    Args:
        address:    an IP address or resolvable hostname to ping
    
    Returns:
        Boolean true if the address can be pinged, false if not
    """
    if LOCAL_SYS == "Windows":
        command = "ping -n 2 {}".format(address)
    elif LOCAL_SYS == "Darwin":
        command = "ping -n -i 1 -c 3 -W 2 {}".format(address)
    else:
        command = "ping -n -i 0.2 -c 5 -W 2 {}".format(address)

    retcode, _, _ = shellutil.Shell(command)
    if retcode == 0:
        return True
    else:
        return False

def SendEmail(emailTo,
              emailSubject,
              emailBody,
              attachments=None,
              emailFrom="testscript@example.com",
              emailServer="aspmx.l.google.com",
              serverUsername=None,
              serverPassword=None):
    """
    Send an email

    Args:
        emailTo:    One or more email addresses to send to
        emailSubject:   the subject line of the email
        emailBody:      the body text of the email
        attachments:    list of file names to attach
        emailFrom:      The email address to use as the sender
        emailServer:    The server to use to send the email
        serverUsername: The username for the email server
        serverPassword: The password for the email server
    """
    if isinstance(emailTo, list):
        send_to = emailTo
    else:
        send_to = []
        send_to.append(emailTo)

    if isinstance(attachments, list):
        attachment_list = attachments
    elif (attachments == None):
        attachment_list = []
    else:
        attachment_list = []
        attachment_list.append(attachments)

    msg = MIMEMultipart()
    msg['From'] = emailFrom
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = emailSubject

    msg.attach(MIMEText(emailBody))

    for filename in attachment_list:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(filename,"rb").read() )
        email.encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(filename))
        msg.attach(part)

    smtp = smtplib.SMTP(emailServer, 25, timeout=30)
    if (serverUsername != None):
        smtp.starttls()
        smtp.login(serverUsername,serverPassword)
    smtp.sendmail(emailFrom, send_to, msg.as_string())
    smtp.close()

def IPToInteger(ip):
    """Convert a string dotted quad IP address to an integer

    Args:
        ipStr: the IP address to convert

    Returns:
        The IP address as an integer
    """
    pieces = ip.split(".")
    return (int(pieces[0]) << 24) + (int(pieces[1]) << 16) + (int(pieces[2]) << 8) + int(pieces[3])

def IntegerToIP(ipInt):
    """Convert an integer IP address to dotted quad notation

    Args:
        ipInt: the IP address to convert

    Returns:
        The IP address as a string in dotted quad notation
    """
    
    return ".".join([str(n) for n in [(ipInt & (0xFF << (8*n))) >> 8*n for n in (3, 2, 1, 0)]])

def CalculateNetwork(ipAddress, subnetMask):
    """Calculate the network given an IP address on the network and the subnet mask of the network

    Args:
        ipAddress: an IP address on the network
        subnetMask: the mask of the network

    Returns:
        The network address in dotted quad notation
    """
    ip_int = IPToInteger(ipAddress)
    mask_int = IPToInteger(subnetMask)
    network_int = ip_int & mask_int
    return IntegerToIP(network_int)

def CalculateBroadcast(ipAddress, subnetMask):
    """Calculate the broadcast address of a network given an IP address on the network and the subnet mask of the network

    Args:
        ipAddress: an IP address on the network
        subnetMask: the mask of the network

    Returns:
        The broadcast address in dotted quad notation
    """
    ip_int = IPToInteger(ipAddress)
    mask_int = IPToInteger(subnetMask)
    bcast_int = ip_int | ~mask_int
    return IntegerToIP(bcast_int)

def CalculateNetmask(startIP, endIP):
    """Calculate the subnet mask of a network given the start and end IP

    Args:
        startIP: the first IP address in the network
        endIP: the last IP address in the network

    Returns:
        The subnet mask in dotted quad notation
    """
    start_ip_int = IPToInteger(startIP)
    end_ip_int = IPToInteger(endIP)
    mask_int = 0xFFFFFFFF ^ start_ip_int ^ end_ip_int
    return IntegerToIP(mask_int)

def IPInNetwork(ipAddress, network):
    """Determine if an IP address is on a given network

    Args:
        ipAddress:  the address to test (str)
        network:    the network (str)

    Returns:
        True if the address is in the network, False if it is not (bool)
    """
    ip_int = IPToInteger(ipAddress)
    net_int = IPToInteger(network)
    return ((ip_int & net_int) == net_int)

def ffs(num):
    """Find the lowest order bit that is set

    Args:
        num: the number to search

    Returns:
        The 0-based index of the lowest order bit that is set, or None if no bits are set
    """
    if num == 0:
        return None
    i = 0
    while (num % 2) == 0:
        i += 1
        num = num >> 1
    return i

def NetmaskToCIDR(netmask):
    """Convert dotted-quad netmask to CIDR

    Args:
        netmask: the string netmask to convert

    Returns:
        The CIDR number corresponding to the netmask
    """
    packed = socket.inet_pton(socket.AF_INET, netmask)
    int_mask = struct.unpack('!I', packed)[0]
    lsb = ffs(int_mask)
    if lsb is None:
        return 0
    cidr_mask = 32 - ffs(int_mask)
    return cidr_mask

def CIDRToNetmask(cidrMask):
    """Convert a CIDR netmask to dotted-quad string

    Args:
        cidrMask: the CIDR netmask to convert

    Returns:
        The dotted-quad string corresponding to the CIDR mask
    """
    bits = 0
    for i in xrange(32 - cidrMask, 32):
        bits |= (1 << i)
    return socket.inet_ntoa(struct.pack('>I', bits))

# Implement inet_pton and inet_ntop for Windows
# From https://gist.github.com/nnemkin/4966028 with minor modifications
if LOCAL_SYS == "Windows":
    class sockaddr(ctypes.Structure):
        _fields_ = [("sa_family", ctypes.c_short),
                    ("__pad1", ctypes.c_ushort),
                    ("ipv4_addr", ctypes.c_byte * 4),
                    ("ipv6_addr", ctypes.c_byte * 16),
                    ("__pad2", ctypes.c_ulong)]

    WSAStringToAddressA = ctypes.windll.ws2_32.WSAStringToAddressA
    WSAAddressToStringA = ctypes.windll.ws2_32.WSAAddressToStringA

    def inet_pton(address_family, ip_string):
        """Convert an IP address from string format to a packed string suitable for use with low-level network functions."""
        addr = sockaddr()
        setattr(addr, "sa_family", address_family)
        addr_size = ctypes.c_int(ctypes.sizeof(addr))

        if WSAStringToAddressA(ip_string, address_family, None, ctypes.byref(addr), ctypes.byref(addr_size)) != 0:
            raise socket.error(ctypes.FormatError())

        if address_family == socket.AF_INET:
            return ctypes.string_at(addr.ipv4_addr, 4)
        if address_family == socket.AF_INET6:
            return ctypes.string_at(addr.ipv6_addr, 16)

        raise socket.error('unknown address family')

    def inet_ntop(address_family, packed_ip):
        """Convert a packed IP address of the given family to string format."""
        addr = sockaddr()
        setattr(addr, "sa_family", address_family)
        addr_size = ctypes.c_int(ctypes.sizeof(addr))
        ip_string = ctypes.create_string_buffer(128)
        ip_string_size = ctypes.c_int(ctypes.sizeof(addr))

        if address_family == socket.AF_INET:
            if len(packed_ip) != ctypes.sizeof(addr.ipv4_addr):
                raise socket.error('packed IP wrong length for inet_ntoa')
            ctypes.memmove(addr.ipv4_addr, packed_ip, 4)
        elif address_family == socket.AF_INET6:
            if len(packed_ip) != ctypes.sizeof(addr.ipv6_addr):
                raise socket.error('packed IP wrong length for inet_ntoa')
            ctypes.memmove(addr.ipv6_addr, packed_ip, 16)
        else:
            raise socket.error('unknown address family')

        if WSAAddressToStringA(ctypes.byref(addr), addr_size, None, ip_string, ctypes.byref(ip_string_size)) != 0:
            raise socket.error(ctypes.FormatError())

        return ip_string[:ip_string_size.value]

    socket.inet_pton = inet_pton
    socket.inet_ntop = inet_ntop
