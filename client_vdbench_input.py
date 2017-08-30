#!/usr/bin/env python2.7

"""
This action will create a vdbench input file for clients
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient, OSType
from libsf.util import ValidateAndDefault, IPv4AddressType, ItemList, BoolType, StrType, PositiveIntegerType, PositiveNonZeroIntegerType
from libsf import sfdefaults
from libsf import SolidFireError
from libsf import threadutil


@logargs
@ValidateAndDefault({
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
    "filename" : (StrType, sfdefaults.vdbench_inputfile),
    "data_errors" : (PositiveIntegerType, sfdefaults.vdbench_data_errors),
    "compratio" : (PositiveIntegerType, sfdefaults.vdbench_compratio),
    "dedupratio" : (PositiveIntegerType, sfdefaults.vdbench_dedupratio),
    "workload" : (StrType, sfdefaults.vdbench_workload),
    "run_time" : (StrType, sfdefaults.vdbench_run_time),
    "interval" : (PositiveNonZeroIntegerType, sfdefaults.vdbench_interval),
    "threads" : (PositiveNonZeroIntegerType, sfdefaults.vdbench_threads),
    "warmup" : (PositiveIntegerType, sfdefaults.vdbench_warmup),
    "data_validation" : (BoolType, sfdefaults.vdbench_data_vaidation),
    "prefill" : (BoolType, False),
    "volume_start" : (PositiveIntegerType, 0),
    "volume_end" : (PositiveIntegerType, 0),
    "local" : (BoolType, False),
})
def ClientVdbenchInput(client_ips,
                       client_user,
                       client_pass,
                       filename,
                       data_errors,
                       compratio,
                       dedupratio,
                       workload,
                       run_time,
                       interval,
                       threads,
                       warmup,
                       data_validation,
                       prefill,
                       volume_start,
                       volume_end,
                       local):
    """
    Create a vdbench input file

    Args:
        client_ips:         the list of client IP addresses
        client_user:        the username for the clients
        client_pass:        the password for the clients
    """
    log = GetLogger()

    log.info("Vuilding vdbench input file")

    # Get a list of vdbench devices from each client
    allgood = True
    results = []
    pool = threadutil.GlobalPool()
    for client_ip in client_ips:
        results.append(pool.Post(_ClientThread, client_ip,
                                                client_user,
                                                client_pass))

    # Write out the vdbench file
    with open(filename, "w") as outfile:
        outfile.write("data_errors={}\n".format(data_errors))
        if data_validation:
            outfile.write("validate=yes\n")
        if compratio > 1:
            outfile.write("compratio={}\n".format(compratio))
        if dedupratio > 1:
            outfile.write("dedupratio={}\n".format(dedupratio))
            outfile.write("dedupunit=4096\n")
        outfile.write("\n")
        outfile.flush()

        host_lines = []
        sd_lines = []
        for client_idx, client_ip in enumerate(client_ips):
            try:
                client_info = results[client_idx].Get()
            except SolidFireError as e:
                log.error("  {}: Failure getting vdbench devices: {}".format(client_ip, e))
                allgood = False
                continue
            
            if not local:
                if client_info["os"] == OSType.Windows:
                    host_lines.append("host=hd{},system={},vdbench=C:\\vdbench,shell=vdbench\n".format(client_idx, client_ip))
                else:
                    host_lines.append("host=hd{},system={},vdbench=/opt/vdbench,user=root,shell=ssh\n".format(client_idx, client_ip))

            for dev_idx, dev in enumerate(client_info["devices"]):
                if volume_start > 0 and dev_idx < volume_start:
                    continue
                if volume_end > 0 and dev_idx > volume_end:
                    continue
                line = "sd=sd{}_{},lun={},openflags=o_direct".format(client_idx, dev_idx, dev)
                if not local:
                    line += ",host=hd{}".format(client_idx)
                line += "\n"
                sd_lines.append(line)
            sd_lines.append("\n")
        if host_lines:
            outfile.writelines(host_lines)
            outfile.write("\n")
        outfile.writelines(sd_lines)
        outfile.flush()

        if prefill:
            outfile.write("wd=prefill,rdpct=0,seekpct=eof,xfersize=256k,sd=sd*")
            if not local:
                outfile.write(",host=hd*")
            outfile.write("\n")

        if data_validation:
            if local:
                outfile.write("wd=wd0,{},sd=sd*\n".format(workload))
            else:
                for client_idx, client_ip in enumerate(client_ips):
                    outfile.write("wd=wd{},host=hd{},{},sd=sd*\n".format(client_idx, client_idx, workload))
                    outfile.flush()
        else:
            outfile.write("wd=wd1,{},sd=sd*".format(workload))
            if not local:
                outfile.write(",host=hd*")
            outfile.write("\n")

        if prefill:
            outfile.write("rd=prefill,wd=prefill,iorate=max,elapsed=200h,threads=4,interval={}\n".format(interval))

        outfile.write("rd=iotest,wd=wd*,iorate=max,elapsed={},interval={},threads={}".format(run_time, interval, threads))
        if warmup > 0:
            outfile.write(",warmup={}".format(warmup))
        outfile.write("\n")

        outfile.flush()

    if allgood:
        log.passed("Successfully created vdbench input file")
        return True
    else:
        log.error("Could not get vdbench devices from all clients")
        return False

@threadutil.threadwrapper
def _ClientThread(client_ip, client_user, client_pass):
    log = GetLogger()
    SetThreadLogPrefix(client_ip)

    result = {}

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)

    result["os"] = client.remoteOS
    result["devices"] = client.GetVdbenchDevices()
    return result


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_client_list_args()
    parser.add_argument("--filename", type=StrType, default=sfdefaults.vdbench_inputfile, help="the file to create")
    parser.add_argument("--data-errors", type=PositiveIntegerType, default=sfdefaults.vdbench_data_errors, help="how many errors to allow before aborting")
    parser.add_argument("--compratio", type=PositiveIntegerType, default=sfdefaults.vdbench_compratio, help="compression ratio to use")
    parser.add_argument("--dedupratio", type=PositiveIntegerType, default=sfdefaults.vdbench_dedupratio, help="dedup ratio to use")
    parser.add_argument("--workload", type=StrType, default=sfdefaults.vdbench_workload, help="workload to use")
    parser.add_argument("--runtime", type=StrType, dest="run_time", default=sfdefaults.vdbench_run_time, help="run time to use")
    parser.add_argument("--interval", type=PositiveNonZeroIntegerType, default=sfdefaults.vdbench_interval, help="reporting interval to use")
    parser.add_argument("--threads", type=PositiveNonZeroIntegerType, default=sfdefaults.vdbench_threads, help="IO threads per volume to use")
    parser.add_argument("--warmup", type=PositiveIntegerType, default=sfdefaults.vdbench_warmup, help="how long a warmup to use")
    parser.add_argument("--no-data-validation", dest="data_validation", action="store_false", default=True, help="skip data validation")
    parser.add_argument("--prefill", action="store_true", default=False, help="run a prefill workload first")
    parser.add_argument("--volume-start", type=PositiveNonZeroIntegerType, default=0, help="sd number to start at")
    parser.add_argument("--volume-end", type=PositiveNonZeroIntegerType, default=0, help="sd number to end at (0 means all sds)")
    parser.add_argument("--local", action="store_true", default=False, help="construct a vdbench file for local IO only")
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientVdbenchInput, args)
    app.Run(**args)
