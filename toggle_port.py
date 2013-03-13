#!/usr/bin/python

# This script will disable and then enable a switch port

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

switch_ip = "10.200.1.230"              # the IP address of the switch
                                        # --switch_ip

switch_user = "vmcert"                  # the switch username
                                        # --switch_user

switch_pass = "password"               # the switch password
                                        # --switch_pass

down_time = 180                         # how long to leave the port down for (seconds)
                                        # the script will fail if this is longer than the
                                        # SSH idle timeout and leave the port disabled
                                        # --down_time

port_desc = "SF-02"                     # the description of the port on the switch.
                                        # Usually this is the node name (SF-xx)
                                        # --port_desc SF-00

port_mac = None                         # the MAC address on the port to disable.  The
                                        # script will search for a port on the switch
                                        # with this MAC attached.
                                        # --port_mac aa:bb:cc:dd:ee:ff

port_name = None                        # the name of the port to disable
                                        # --port_name "TenGigabitEthernet 0/0"

# ----------------------------------------------------------------------------

from optparse import OptionParser
import subprocess
import sys,os
import re
import time
import libsf
from libsf import mylog


def WaitForPrompt(process, prompt):
    current_char = ''
    buff = ''
    while True:
        current_char = process.stdout.read(1)
        buff += current_char
        if buff.endswith(str(prompt)): break
    #print "Found a match for " + str(prompt)
    return buff

def SelectPort(process, portname):
    #print "sending command 'config'"
    process.stdin.write("config\n")
    #print "Waiting for prompt"
    WaitForPrompt(process, "#")
    #print "sending command 'interface " + port_name + "'"
    process.stdin.write("interface " + port_name + "\n")
    #print "Waiting for prompt"
    WaitForPrompt(process, "#")


def main():
    parser = OptionParser()
    global switch_ip, switch_user, switch_pass, down_time, port_desc, port_mac, port_name
    parser.add_option("--switch_ip", type="string", dest="switch_ip", default=switch_ip, help="the IP addresses of the switch")
    parser.add_option("--switch_user", type="string", dest="switch_user", default=switch_user, help="the user name for the switch")
    parser.add_option("--switch_pass", type="string", dest="switch_pass", default=switch_pass, help="the password for the switch")
    parser.add_option("--down_time", type="float", dest="down_time", default=down_time, help="how long to leave the port down (sec)")
    parser.add_option("--port_desc", type="string", dest="port_desc", default=port_desc, help="the description of the port on the switch to be disabled")
    parser.add_option("--port_mac", type="string", dest="port_mac", default=port_mac, help="the MAC connected to the port on the switch to be disabled")
    parser.add_option("--port_name", type="string", dest="port_name", default=port_name, help="the name of the port on the switch to be disabled")

    (options, args) = parser.parse_args()
    switch_ip = options.switch_ip
    switch_user = options.switch_user
    switch_pass = options.switch_pass
    down_time = float(options.down_time)
    port_desc = options.port_desc
    port_mac = options.port_mac
    port_name = options.port_name

    mylog.info("Connecting to switch '" + switch_ip + "'...")
    p = subprocess.Popen("ssh " + switch_user + "@" + switch_ip, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    #print "Waiting for prompt"
    WaitForPrompt(p, "#")

    if port_name == None:
        if port_desc != None:
            mylog.info("Finding port with description " + port_desc)
            #print "sending command show interfaces description | no-more | grep " + port_desc + " ignore-case"
            p.stdin.write("show interfaces description | no-more | grep " + port_desc + " ignore-case\n")
            data = WaitForPrompt(p, "#")
            for line in data.split("\n"):
                m = re.search("(TenGigabitEthernet \d/\d)\s+.+(" + port_desc + ":eth\d)", line, re.IGNORECASE)
                if (m):
                    port_name = m.group(1)
            if port_name == None:
                mylog.error("Could not find port with description '" + port_desc + "'")
                exit(1)
        else:
            mylog.info("Finding port with MAC " + port_mac)
            #print "sending command show mac-address-table address " + port_mac
            p.stdin.write("show mac-address-table address " + port_mac + "\n")
            data = WaitForPrompt(p, "#")
            for line in data.split("\n"):
                m = re.search("\s*(\d+)\s+([a-f0-9:]+)\s+(\S+)\s+Te (\d/\d)\s+(\S+)", line, re.IGNORECASE)
                if (m and m.group(2).lower() == port_mac.lower()):
                    port = m.group(4)
                    port_name = "TenGigabitEthernet " + port
            if port_name == None:
                mylog.error("Could not find port with MAC '" + port_mac + "'")
                exit(1)

    mylog.info("Selecting port " + port_name)
    SelectPort(p, port_name)

    mylog.info("Shutting down port")
    p.stdin.write("shutdown\n")
    #print "Waiting for prompt"
    WaitForPrompt(p, "#")

    mylog.info("Waiting for " + str(down_time) + " sec...")
    time.sleep(down_time)

    mylog.info("Bringing up port")
    p.stdin.write("no shutdown\n")
    WaitForPrompt(p, "#")

    #print "sending exit"
    p.stdin.write("exit\n")
    #print "Waiting for prompt"
    WaitForPrompt(p, "#")
    #print "sending exit"
    p.stdin.write("exit\n")
    #print "Waiting for prompt"
    WaitForPrompt(p, "#")
    #print "sending exit"
    p.stdin.write("exit\n")
    #print "waiting for finish"
    p.wait()


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
