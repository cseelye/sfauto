#!/usr/bin/env python2.7

"""
This action will force a whole file sync on the given volumes
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs, SetThreadLogPrefix
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, PositiveIntegerType, TimestampToStr, OptionalValueType, ItemList, StrType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError
import os
import tarfile
import time
from io import open

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "dest" : (StrType, "bundles"),
    "label" : (StrType, False),
    "interval" : (PositiveIntegerType, 0),
    "reports" : (OptionalValueType(ItemList(StrType)), None),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClusterSaveReports(dest,
                       label,
                       interval,
                       reports,
                       archive,
                       mvip,
                       username,
                       password):
    """
    Save reports from the cluster into files

    Args:
        dest:           directory to save reports in
        label:          label to prepend to the report filename
        interval:       how often to save reports (sec). Use 0 to only save once
        reports:        list of reports to save
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    if not os.path.exists(dest):
        os.makedirs(dest)

    cluster = SFCluster(mvip, username, password)

    while True:
        if not reports:
            reports = cluster.ListReports()

            # Blacklist a few reports
            reports = [rep for rep in reports if rep not in ["events",
                                                             "mutexes",
                                                             "rpcTimeReset"]]

        log.info("Saving reports")
        timestamp = TimestampToStr(time.time(), "%Y-%m-%d_%H.%M.%S")
        pool = threadutil.ThreadPool(maxThreads=len(reports))
        results = []
        report_files = []
        for rep in reports:
            filename = "{}/{}{}_{}.html".format(dest,
                                      "{}_".format(label) if label else "",
                                      timestamp,
                                      rep)
            report_files.append(filename)
            results.append(pool.Post(_ReportThread, mvip, username, password, rep, filename))

        allgood = True
        for idx, rep in enumerate(reports):
            try:
                results[idx].Get()
            except SolidFireError as e:
                log.error("  Error getting report {}: {}".format(rep, e))
                allgood = False
                continue

        if allgood:
            log.passed("Successfully saved all reports")
        else:
            log.error("Could not save all reports")

        if interval <= 0:
            if allgood:
                return True
            else:
                return False

        if archive and len(report_files) > 0:
            log.info("Creating tarball...")
            tarname = "{}/{}{}_reports.tar.gz".format(dest,
                                      "{}_".format(label) if label else "",
                                      timestamp)
            with tarfile.TarFile(tarname, "w:gz") as tar:
                for filename in report_files:
                    if os.path.exists(filename):
                        tar.add(filename)
            for filename in report_files:
                if os.path.exists(filename):
                    os.unlink(filename)

        log.info("Waiting {} seconds...".format(interval))
        time.sleep(interval)


@threadutil.threadwrapper
def _ReportThread(mvip, username, password, report, filename):
    """Force syncing on a volume"""
    log = GetLogger()
    SetThreadLogPrefix(report)

    log.info("Getting report from cluster")
    report_html = SFCluster(mvip, username, password).GetReport(report)
    with open(filename, "w") as outfile:
        outfile.write(report_html)


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--dest", default="bundles", help="the name of the directory to store the reports in")
    parser.add_argument("--label", help="label to prepend to the name of the report file")
    parser.add_argument("--interval", type=PositiveIntegerType, default=0, help="how long to wait between each round of gathering reports (sec). Use 0 to only save one time instead of in a loop")
    parser.add_argument("--reports", help="list of reports to save. Default is all except the event and mutex timing reports")
    parser.add_argument("--archive", action="store_true", default=False, help="tarball the reports gathered on each interval")
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterSaveReports, args)
    app.Run(**args)
