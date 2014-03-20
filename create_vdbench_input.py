#!/usr/bin/python

"""
This action will create a simple vdbench input file based on the iscsi volumes connected to a list of clients

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --filename          The name of the file to create

    --workload          The workload specification

    --data_errors       The number of errors to halt the test after

    --compratio         The compression ratio to use

    --dedupratio        The dedup ratio to use

    --run_time          How long to run IO

    --interval          How often to report results

    --threads           Queue depth per device

    --nodatavalidation  Skip data validation

    --volume_start      Volume number to start from

    --volume_end        Volume to end at
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import SfClient, ClientError, OsType
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CreateVdbenchInputAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"client_ips" : libsf.IsValidIpv4AddressList,
                            "filename" : None},
            args)

    def Execute(self, client_ips, filename, data_errors=sfdefaults.vdbench_data_errors, compratio=sfdefaults.vdbench_compratio, dedupratio=sfdefaults.vdbench_dedupratio, workload=sfdefaults.vdbench_workload, run_time=sfdefaults.vdbench_run_time, interval=sfdefaults.vdbench_interval, threads=sfdefaults.vdbench_threads, warmup=sfdefaults.vdbench_warmup, datavalidation=sfdefaults.vdbench_data_vaidation, volume_start=0, volume_end=0, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Create a vdbench input file
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        try:
            # Open output file and write common params and hds
            outfile = open(filename, 'w')
            outfile.write("data_errors=" + str(data_errors) + "\n")
            if datavalidation:
                outfile.write("validate=read_after_write" + "\n")
            if compratio > 1:
                outfile.write("compratio=" + str(compratio) + "\n")
            if dedupratio > 1:
                outfile.write("dedupratio=" + str(dedupratio) + "\n")
                outfile.write("dedupunit=4096\n")

            # Connect to clients
            clients = dict()
            for client_ip in client_ips:
                mylog.info("Connecting to " + client_ip)
                client = SfClient()
                try:
                    client.Connect(client_ip, client_user, client_pass)
                except ClientError as e:
                    mylog.error(e.message)
                    sys.exit(1)
                clients[client_ip] = client

            # Write the correct hd line for each client based on client OS
            host_number = 1
            for client_ip in client_ips:
                client = clients[client_ip]
                if client.RemoteOs == OsType.Windows:
                    outfile.write("hd=hd" + str(host_number) + ",system=" + client_ip + ",vdbench=C:\\vdbench,shell=vdbench\n")
                else:
                    outfile.write("hd=hd" + str(host_number) + ",system=" + client_ip + ",vdbench=/opt/vdbench,user=root,shell=ssh\n")
                outfile.flush()
                host_number += 1

            # Connect to each client and build the list of SDs
            host_number = 1
            for client_ip in client_ips:
                client = clients[client_ip]
                mylog.info("Querying connected volumes on " + client.Hostname + "")
                devices = client.GetVdbenchDevices()
                if volume_start <= 0:
                    volume_start = 1
                if volume_end <= 0:
                    volume_end = len(devices)
                outfile.write("\n# sd devices for host hd" + str(host_number) + " (" + client_ip + ")\n")
                for sd_number in xrange(volume_start, volume_end + 1):
                    device = devices[sd_number - 1]
                    outfile.write("sd=sd" + str(host_number) + "_" + str(sd_number) + ",host=hd" + str(host_number))
                    if client.RemoteOs == OsType.Windows:
                        outfile.write(",lun=" + device + "\n")
                    elif client.RemoteOs == OsType.SunOS:
                        outfile.write(",lun=" + device + "\n")
                    else:
                        outfile.write(",lun=" + device + ",openflags=o_direct\n")
                    outfile.flush()
                host_number += 1

            outfile.write("wd=default," + workload + ",sd=sd*\n")
            host_number = 1
            for client_ip in client_ips:
                outfile.write("wd=wd" + str(host_number) + ",host=hd" + str(host_number) + "\n")
                outfile.flush()
                host_number += 1

            outfile.write("rd=default,iorate=max,elapsed=" + str(run_time) + ",interval=" + str(interval) + ",threads=" + str(threads))
            if warmup > 0:
                outfile.write(",warmup=" + str(warmup))
            outfile.write("\n")
            outfile.write("rd=rd1,wd=wd*" + "\n")
            outfile.flush()
            outfile.close()
            return True
        except KeyboardInterrupt:
            raise

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-c", "--client_ips", action="list", dest="client_ips", default=sfdefaults.client_ips, help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("--filename", type="string", dest="filename", default=sfdefaults.vdbench_inputfile, help="the file to create [%default]")
    parser.add_option("--data_errors", type="string", dest="data_errors", default=sfdefaults.vdbench_data_errors, help="the workload specification [%default]")
    parser.add_option("--compratio", type="int", dest="compratio", default=sfdefaults.vdbench_compratio, help="the compression ratio to use [%default]")
    parser.add_option("--dedupratio", type="int", dest="dedupratio", default=sfdefaults.vdbench_dedupratio, help="the dedupratio ratio to use [%default]")
    parser.add_option("--workload", type="string", dest="workload", default=sfdefaults.vdbench_workload, help="the workload specification [%default]")
    parser.add_option("--run_time", type="string", dest="run_time", default=sfdefaults.vdbench_run_time, help="the run time (how long to run vdbench/IO) [%default]")
    parser.add_option("--interval", type="int", dest="interval", default=sfdefaults.vdbench_interval, help="how often to report results to the screen [%default]")
    parser.add_option("--threads", type="int", dest="threads", default=sfdefaults.vdbench_threads, help="how many threads per sd (queue depth) [%default]")
    parser.add_option("--warmup", type="int", dest="warmup", default=sfdefaults.vdbench_warmup, help="how long a warmup period [%default]")
    parser.add_option("--nodatavalidation", action="store_false", dest="datavalidation", default=True, help="skip data validation [%default]")
    parser.add_option("--volume_start", type="int", dest="volume_start", default=0, help="sd number to start at [%default]")
    parser.add_option("--volume_end", type="int", dest="volume_end", default=0, help="sd number to finish at (0 means all sds) [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ips, options.filename, options.data_errors, options.compratio, options.dedupratio, options.workload, options.run_time, options.interval, options.threads, options.warmup, options.datavalidation, options.volume_start, options.volume_end, options.client_user, options.client_pass, options.debug):
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
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
