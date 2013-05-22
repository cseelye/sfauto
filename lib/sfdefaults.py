#!/usr/bin/env python
"""
Default values for all actions to use

This module will replace the defaults here with values from your environment if present.
To use this functionality, export an environment variable called "SF" + the name of the default to override
For example,
    export SFMVIP=1.2.3.4
    export SFCLIENT_IPS=1.1.1.1,2.2.2.2
"""

# =============================================================================
# Default Behaviors
stop_on_error = False               # Behavior when an error occurs
# =============================================================================
# Default Values
mvip = None                         # Cluster MVIP
svip = None                         # Cluster SVIP
username = "admin"                  # Cluster admin username
password = "admin"                  # Cluster admin password
node_ips = []                       # List of node IP addresses
ssh_user = "root"                   # Cluster node SSH username
ssh_pass = "password"               # Cluster node ssh password
ipmi_user = "root"                  # IPMI username
ipmi_pass = "root"                  # IPMI password
min_iops = 100                      # QoS nin IOPS
max_iops = 100000                   # QoS max IOPS
burst_iops = 100000                 # QoS burst IOPS
bin_sync_timeout = 3600             # Bin sync timeout, sec
slice_sync_timeout = 3600           # Slice sync timeout, sec
fill_timeout = 43200                # Clustr fill timeout, sec
client_ips = []                     # List of client IP addresses
client_user = "root"                # Client SSH username
client_pass = "password"            # Client SSH password
client_volume_sort = "iqn"          # Sort order for displaying client volumes
login_order = "serial"              # Order to log in to volumes on the client (serial or parallel)
auth_type = "chap"                  # iSCSI auth type - chap or none
parallel_thresh = 5                 # Run multi-client actions in parallel if there are more than this many
parallel_max = 20                   # Run at most this many multi-client actions in parallel
parallel_calls_thresh = 2           # Run multiple API cals in parallel if there are mode than this many
parallel_calls_max = 100            # Run at most this many API calls in parallel
fault_whitelist = [                 # Ignore these faults if they are present on the cluster
    "clusterFull",
    "clusterIOPSAreOverProvisioned",
    "nodeHardwareFault"
]
volume_access = "readWrite"         # Volume access level
vdbench_inputfile = "vdbench_input" # Input file for vdbench
vdbench_outputdir = "output.tod"    # Output directory for vdbench
vdbench_workload = "rdpct=50,seekpct=random,xfersize=8k"    # IO workload to run
vdbench_data_errors = 5             # How many IO errors to fail vdbench after
vdbench_compratio = 2               # IO compression ratio
vdbench_dedupratio = 1              # IO dedup ratio
vdbench_run_time = "800h"           # How long to run IO
vdbench_interval = 10               # How often to report results
vdbench_threads = 4                 # Queue depth per device
vdbench_data_vaidation = True       # Use data validation
email_notify = None                 # List of email addresses to notification
email_from = "testscript@nothing"   # Email address to send notifications from
smtp_server = "aspmx.l.google.com"  # SMTP server for sending email
gc_timeout = 60                     # How long to wait (min) for GC to finish
host_user = "root"                  # Hypervisor host username
host_pass = "password"              # Hypervisor host password
vmhost_kvm = None                   # KVM hypervisor host
vmhost_xen = None                   # XenServer hypervisor host
xenapi_parallel_calls_thresh = 2    # Run multiple XenServer API operations in parallel if there are more than this many
xenapi_parallel_calls_max = 5       # Run at most this many parallel operations with XenServer API
# =============================================================================
# Default Choices
all_volume_acess_levels = [         # Valid volume access levels
    "readWrite",
    "readOnly",
    "locked"
]
all_admin_access_levels = [         # Valid cluster admin access levels
    "read",
    "write",
    "administrator",
    "reporting",
    "drives",
    "volumes",
    "accounts",
    "nodes",
    "clusteradmins",
    "repositories"
]
all_auth_types = [                  # Valid iSCSI auth types
    "chap",
    "none"
]
all_login_orders = [                # Valid login orders
    "serial",
    "parallel"
]
all_client_volume_sort = [          # Valid sort orders for client volumes
    "iqn",
    "device",
    "portal",
    "state"
]
# =============================================================================

__var_names = dir()

import os as __os
import sys as __sys
import inspect as __inspect
__thismodule = __sys.modules[__name__]

# Create an entry for each default with the original value
for __name in __var_names:
    __obj = getattr(__thismodule, __name)
    if __inspect.isbuiltin(__obj): continue
    if __inspect.ismodule(__obj): continue
    if __name.startswith("__"): continue

    setattr(__thismodule, __name + "_orig", __obj)
del __obj

# Update the defaults from the user's environment
for __name in __var_names:
    __obj = getattr(__thismodule, __name)
    if __inspect.isbuiltin(__obj): continue
    if __inspect.ismodule(__obj): continue
    if __name.startswith("__"): continue

    __env_name = "SF" + __name.upper()
    if __os.environ.get(__env_name):
        # See if this is supposed to be a list or scalar
        __current_value = getattr(__thismodule, __name)
        __list_type = False
        if isinstance(__current_value, list):
            __list_type = True
        if __list_type:
            from string import strip as __strip
            __new_val = map(__strip, __os.environ[__env_name].split(","))
            setattr(__thismodule, __name, __new_val)
        else:
            setattr(__thismodule, __name, __os.environ[__env_name])
del __env_name
del __name


def GetDefaults():
    mydefaults = dict()
    for name in __var_names:
        obj = getattr(__thismodule, name)
        if __inspect.isbuiltin(obj): continue
        if __inspect.ismodule(obj): continue
        if __inspect.ismethod(obj): continue
        if __inspect.isfunction(obj): continue
        if name.startswith("__"): continue
        if name.endswith("_orig"): continue
        mydefaults[name] = obj
    return mydefaults

def PrintDefaults():
    defaults = GetDefaults()
    for name in sorted(defaults.keys()):
        print "%20s" % name + "  =>  " + str(defaults[name])

