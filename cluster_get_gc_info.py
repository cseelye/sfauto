#!/usr/bin/env python2.7

"""
This action will display the GC info from the cluster
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, StrType
from libsf.util import TimestampToStr, HumanizeBytes, SecondsToElapsedStr
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClusterGetGCInfo(mvip,
                     username,
                     password):
    """
    Display the GC info

    Args:
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)
    try:
        gc_list = cluster.GetAllGCInfo()
    except SolidFireError as ex:
        log.error(ex)
        return False
    
    for gc_info in gc_list:
        if gc_info.Rescheduled:
            log.warning("GC generation " + str(gc_info.Generation) + " started " + TimestampToStr(gc_info.StartTime) + " was rescheduled")
        elif gc_info.EndTime <= 0:
            log.warning("GC generation " + str(gc_info.Generation) + " started " + TimestampToStr(gc_info.StartTime) + " did not complete ")
            log.warning("    " + str(len(gc_info.ParticipatingSSSet)) + " participating SS: [" + ",".join(map(str, gc_info.ParticipatingSSSet)) + "]  " + str(len(gc_info.EligibleBSSet)) + " eligible BS: [" + ",".join(map(str, gc_info.EligibleBSSet)) + "]")
            log.warning("    " + str(len(gc_info.EligibleBSSet - gc_info.CompletedBSSet)) + " BS did not complete GC: [" + ", ".join(map(str, gc_info.EligibleBSSet - gc_info.CompletedBSSet)) + "]")
        else:
            log.info("GC generation " + str(gc_info.Generation) + " started " + TimestampToStr(gc_info.StartTime) + ", duration " + SecondsToElapsedStr(gc_info.EndTime - gc_info.StartTime) + ", " + HumanizeBytes(gc_info.DiscardedBytes) + " discarded")
            log.info("    " + str(len(gc_info.ParticipatingSSSet)) + " participating SS: " + ",".join(map(str, gc_info.ParticipatingSSSet)) + "  " + str(len(gc_info.EligibleBSSet)) + " eligible BS: " + ",".join(map(str, gc_info.EligibleBSSet)) + "")
        log.blankline()

    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterGetGCInfo, args)
    app.Run(**args)
