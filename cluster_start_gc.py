#!/usr/bin/env python2.7

"""
This action will start GC on the cluster
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, PositiveNonZeroIntegerType, BoolType, StrType
from libsf.util import TimestampToStr, HumanizeBytes, SecondsToElapsedStr
from libsf import sfdefaults
from libsf import SolidFireError, TimeoutError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "force" : (BoolType, False),
    "wait" : (BoolType, False),
    "gc_timeout" : (PositiveNonZeroIntegerType, sfdefaults.gc_timeout),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def StartGC(force,
            wait,
            gc_timeout,
            mvip,
            username,
            password):
    """
    Start GC on the cluster

    Args:
        force:              try to start GC even if one is in progress
        wait:               wait for GC to complete
        gc_timeout:         how long to wait before giving up
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)
    try:
        cluster.StartGC(force)
    except SolidFireError as ex:
        log.error(ex)
        return False

    if wait:
        log.info("Waiting for GC to finish")
        try:
            gc_info = cluster.WaitForGC(gc_timeout)
        except TimeoutError:
            log.error("Timeout waiting for GC to finish")
            return False
        except SolidFireError as ex:
            log.error(ex)
            return False

        if gc_info.Rescheduled:
            log.warning("GC generation " + str(gc_info.Generation) + " started " + TimestampToStr(gc_info.StartTime) + " was rescheduled")
        elif gc_info.EndTime <= 0:
            log.warning("GC generation " + str(gc_info.Generation) + " started " + TimestampToStr(gc_info.StartTime) + " did not complete ")
            log.warning("    " + str(len(gc_info.ParticipatingSSSet)) + " participating SS: [" + ",".join(map(str, gc_info.ParticipatingSSSet)) + "]  " + str(len(gc_info.EligibleBSSet)) + " eligible BS: [" + ",".join(map(str, gc_info.EligibleBSSet)) + "]")
            log.warning("    " + str(len(gc_info.EligibleBSSet - gc_info.CompletedBSSet)) + " BS did not complete GC: [" + ", ".join(map(str, gc_info.EligibleBSSet - gc_info.CompletedBSSet)) + "]")
        else:
            log.info("GC generation " + str(gc_info.Generation) + " started " + TimestampToStr(gc_info.StartTime) + ", duration " + SecondsToElapsedStr(gc_info.EndTime - gc_info.StartTime) + ", " + HumanizeBytes(gc_info.DiscardedBytes) + " discarded")
            log.info("    " + str(len(gc_info.ParticipatingSSSet)) + " participating SS: " + ",".join(map(str, gc_info.ParticipatingSSSet)) + "  " + str(len(gc_info.EligibleBSSet)) + " eligible BS: " + ",".join(map(str, gc_info.EligibleBSSet)) + "")


    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--force", action="store_true", default=False, help="try to start GC even if one is already in progress")
    parser.add_argument("--wait", action="store_true", default=False, help="wait for GC to complete")
    parser.add_argument("--timeout", dest="gc_timeout", type=PositiveNonZeroIntegerType, default=sfdefaults.gc_timeout, metavar="MINUTES", help="how long to wait before giving up, in minutes")
    args = parser.parse_args_to_dict()

    app = PythonApp(StartGC, args)
    app.Run(**args)
