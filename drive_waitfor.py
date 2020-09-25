#!/usr/bin/env python2.7

"""
This action will wait for drives to be in the given state
"""
#pylint: disable=eval-used

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, CountType, ItemList, SelectionType, StrType, PositiveIntegerType
from libsf import sfdefaults
from libsf import SolidFireError
import time

@logargs
@ValidateAndDefault({
    "expected" : (CountType(allowZero=True), None),
    "states" : (ItemList(SelectionType(sfdefaults.all_drive_states)), None),
    "compare" : (SelectionType(list(sfdefaults.all_compare_ops.keys())), "ge"),
    "timeout" : (PositiveIntegerType, sfdefaults.available_drives_timeout),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def DriveWaitfor(expected,
                 states,
                 compare,
                 timeout,
                 mvip,
                 username,
                 password):
    """
    Wait for at least the specified number of available drives in the cluster

    Args:
        expected:       the expected number of drives
        timeout:        how long to wait before giving up
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()
    op = sfdefaults.all_compare_ops[compare]

    log.info("Waiting for {}{} drives in state [{}]...".format(op, expected, ",".join(states)))
    start_time = time.time()
    last_count = 0
    while True:
        if time.time() - start_time > timeout:
            log.error("Timeout waiting for drives")
            return False

        try:
            drives = SFCluster(mvip, username, password).ListDrives(driveState=states)
        except SolidFireError as e:
            log.error("Failed to list drives: {}".format(e))
            return False

        if len(drives) != last_count:
            log.info("  Found {} drives".format(len(drives)))
            last_count = len(drives)

        expression = "{}{}{}".format(len(drives), op, expected)
        log.debug("Evaluating expression {}".format(expression))
        result = eval(expression)

        if result:
            log.passed("Successfully waited for drives")
            return True

        time.sleep(sfdefaults.TIME_SECOND * 20)


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--expected", type=CountType(), metavar="COUNT", required=True, help="the number of drives to wait for")
    parser.add_argument("--states", type=ItemList(str), required=True, help="the drive state to count")
    parser.add_argument("--compare", type=str, choices=sfdefaults.all_compare_ops, default="ge", metavar="OP", required=True, help="the compare operation to use between actual and expected drives")
    parser.add_argument("--timeout", type=CountType(), default=sfdefaults.available_drives_timeout, metavar="SECONDS", required=True, help="how long to wait before giving up")
    args = parser.parse_args_to_dict()

    app = PythonApp(DriveWaitfor, args)
    app.Run(**args)
