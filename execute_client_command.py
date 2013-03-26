#!/usr/bin/python

# This script will execute a command on a client and print the result

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

client_ip = "192.168.000.000"       # The IP address of the client
                                    # --client_ips

client_user = "root"                # The username for the client
                                    # --client_user

client_pass = "password"           # The password for the client
                                    # --client_pass

command = ""                        # The command to execute on the client
                                    # --command

csv = False                         # Display minimal output that is suitable for piping into other programs
                                    # --csv

bash = False                        # Display minimal output that is formatted for a bash array/for  loop
                                    # --bash

# ----------------------------------------------------------------------------


from optparse import OptionParser
import paramiko
import sys,os
import libsf
from libsf import mylog
import libclient
from libclient import ClientError, SfClient

def main():
    global client_ip, client_user, client_pass, command

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "client_ip", "client_user", "client_pass" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--client_ip", type="string", dest="client_ip", default=client_ip, help="the IP address of the client")
    parser.add_option("--client_user", type="string", dest="client_user", default=client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=client_pass, help="the password for the client [%default]")
    parser.add_option("--command", type="string", dest="command", default=command, help="the command to execute on the client")
    parser.add_option("--csv", action="store_true", dest="csv", help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    client_user = options.client_user
    client_pass = options.client_pass
    client_ip = options.client_ip
    command = options.command
    bash = options.bash
    csv = options.csv
    if not libsf.IsValidIpv4Address(client_ip):
        mylog.error("'" + client_ip + "' does not appear to be a valid client IP address")
        sys.exit(1)
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not command:
        mylog.error("Please specify a command to execute on the client")
        sys.exit(1)
    if bash or csv:
        mylog.silence = True

    return_code = None
    stdout = ""
    stderr = ""
    try:
        mylog.info("Connecting to " + client_ip)
        client = SfClient()
        client.Connect(client_ip, client_user, client_pass)
        mylog.info("Executing '" + command + "' on " + client_ip)
        return_code, stdout, stderr = client.ExecuteCommand(command)
    except ClientError as e:
        mylog.error(str(e))
        sys.exit(1)
    stdout = stdout.rstrip("\n")
    stderr = stderr.rstrip("\n")

    if bash or csv:
        sys.stdout.write(stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()
    else:
        mylog.info("Return code: " + str(return_code))
        mylog.info("STDOUT: " + stdout)
        if stderr:
            mylog.info("STDERR: " + stderr)

    if return_code != None:
        sys.exit(return_code)
    else:
        sys.exit(-1)


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
