#!/usr/bin/python

"""
This action will show the FC HBA infor on clients

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients
    SFCLIENT_IPS env var

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var
"""


from optparse import OptionParser
import re
import sys
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import SfClient, ClientError, OsType
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class ShowClientHbasAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientIPs" : libsf.IsValidIpv4AddressList},
            args)

    def pretty_wwn(ugly_wwn):
        pretty = ''
        for i in range(2, 2*8+2, 2):
            pretty += ':' + ugly_wwn[i:i+2]
        return pretty[1:]

    def Execute(self, clientIPs=None, sort=sfdefaults.client_volume_sort, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, debug=False):
        """
        Show the HBAs in a client
        """
        if not clientIPs:
            clientIPs = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Gathering info from clients...")
        info = {}
        for client_ip in clientIPs:
            client = SfClient()
            try:
                client.Connect(client_ip, clientUser, clientPass)
            except ClientError as e:
                mylog.error(client_ip + ": " + e.message)
                continue

            hbas = {}
            cmd = "ls /sys/class/fc_host"
            return_code, stdout, stderr = client.ExecuteCommand(cmd)
            for line in stdout.split("\n"):
                line = line.strip()
                if not line:
                    continue
                host = line
                m = re.search("(\d+)", host)
                host_num = m.group(1)
                hbas[host] = {}
                cmd = "[ -e /sys/class/fc_host/" + host + "/device/scsi_host/" + host + "/modeldesc ] && cat /sys/class/fc_host/" + host + "/device/scsi_host/" + host + "/modeldesc || cat /sys/class/fc_host/" + host + "/device/scsi_host/" + host + "/model_desc"
                return_code1, stdout1, stderr1 = client.ExecuteCommand(cmd)
                hbas[host]['desc'] = stdout1.strip()
                cmd = "cat /sys/class/fc_host/" + host + "/port_name"
                return_code1, stdout1, stderr1 = client.ExecuteCommand(cmd)
                hbas[host]['wwn'] = libsf.HumanizeWWN(stdout1.strip())
                cmd = "cat /sys/class/fc_host/" + host + "/speed"
                return_code1, stdout1, stderr1 = client.ExecuteCommand(cmd)
                hbas[host]['speed'] = stdout1.strip()
                cmd = "cat /sys/class/fc_host/" + host + "/device/scsi_host/" + host + "/link_state"
                return_code1, stdout1, stderr1 = client.ExecuteCommand(cmd)
                link_state = stdout1.strip()
                if "-" in link_state:
                    link_state = link_state[:link_state.index("-")-1].strip()
                hbas[host]['link'] = link_state
                cmd = "for port in `ls -1d /sys/class/fc_remote_ports/rport-" + host_num + "*`; do a=$(cat $port/roles); if [[ $a == *Target* ]]; then cat $port/port_name; fi; done"
                return_code1, stdout1, stderr1 = client.ExecuteCommand(cmd)
                hbas[host]['targets'] = []
                for line1 in stdout1.split("\n"):
                    line1 = line1.strip()
                    if not line1:
                        continue
                    hbas[host]['targets'].append(libsf.HumanizeWWN(line1.strip()))

            info[client_ip] = hbas

        for ip in info.keys():
            mylog.info("")
            mylog.info(ip + " has " + str(len(hbas)) + " FC HBAs")
            hbas = info[ip]
            for host in sorted(hbas.keys()):
                hba = hbas[host]
                mylog.info("  " + host + "  " + hba['desc'] + "  " + hba['wwn'] + "  " + hba['link'] + "  " + hba['speed'])
                for targ in hba['targets']:
                    mylog.info("    Target " + targ)


        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-c", "--client_ips", action="list", dest="client_ips", default=None, help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("--sort", type="choice", choices=sfdefaults.all_client_volume_sort, dest="sort", default=sfdefaults.client_volume_sort, help="the sort order to display the volumes [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ips, options.sort, options.client_user, options.client_pass, options.debug):
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
