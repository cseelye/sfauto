#!/usr/bin/env python

"""
This action will count the number of drives in the given state and compare to the expected number
"""
#pylint: disable=eval-used

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, CountType, SelectionType, StrType
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    "expected" : (CountType(allowZero=True), None),
    "state" : (SelectionType(sfdefaults.all_drive_states), None),
    "compare" : (SelectionType(list(sfdefaults.all_compare_ops.keys())), "eq"),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def DriveVerifyCount(expected,
                     state,
                     compare,
                     mvip,
                     username,
                     password):
    """
    Count drives in the cluster in a given state

    Args:
        expected:       the expected number of drives
        state:          the drive state to count
        compare:        the compare operation to use between actual and expected drives
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()
    op = sfdefaults.all_compare_ops[compare]

    # Get the list of drives
    try:
        drives = SFCluster(mvip, username, password).ListDrives(driveState=state)
    except SolidFireError as e:
        log.error("Failed to list drives: {}".format(e))
        return False

    expression = "{}{}{}".format(len(drives), op, expected)
    log.debug("Evaluating expression {}".format(expression))
    result = eval(expression)
    if result:
        log.passed("Found {} drives in {} state".format(len(drives), state))
        return True
    else:
        log.error("Found {} drives in {} state".format(len(drives), state))
        return False

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--expected", type=CountType(allowZero=True), metavar="COUNT", required=True, help="the expected number of drives")
    parser.add_argument("--compare", type=str, choices=sfdefaults.all_compare_ops, default="eq", metavar="OP", required=True, help="the compare operation to use between actual and expected drives")
    parser.add_argument("--state", type=str, choices=sfdefaults.all_drive_states, required=True, help="the drive state to count")
    args = parser.parse_args_to_dict()

    app = PythonApp(DriveVerifyCount, args)
    app.Run(**args)
