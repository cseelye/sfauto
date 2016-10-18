#!/usr/bin/env python2.7
"""
Default values for all actions to use

This module will replace the defaults here with values from your environment if present.
To use this functionality, export an environment variable called "SF" + the name of the default to override
For example,
    export SFMVIP=1.2.3.4
    export SFCLIENT_IPS=1.1.1.1,2.2.2.2


DO NOT USE AN EMPTY LIST [] OR EMPTY DICT {} FOR ANY OF THESE VALUES!!!!!

"""

# =============================================================================
# Default Behaviors

stop_on_error = False               # Behavior when an error occurs
use_multiprocessing = False         # Use multiprocessing instead of multithreading
all_api_versions = [                # All known endpoint versions
    0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 7.1, 7.2, 7.3, 7.4, 8.0, 8.1, 8.2, 8.3, 8.4, 9.0]
parallel_thresh = 5                 # Run multi-client actions in parallel if there are more than this many
parallel_max = 20                   # Run at most this many multi-client actions in parallel
parallel_calls_min = 2              # Run multiple operations in parallel if there are at least this many
parallel_calls_max = 100            # Run at most this many operations in parallel
xenapi_parallel_calls_thresh = 2    # Run multiple XenServer API operations in parallel if there are more than this many
xenapi_parallel_calls_max = 5       # Run at most this many parallel operations with XenServer API

# =============================================================================
# Default Values
#
# DO NOT USE AN EMPTY LIST [] OR EMPTY DICT {} FOR ANY OF THESE VALUES
#

# CLuster/node
mvip = None                         # Cluster MVIP
svip = None                         # Cluster SVIP
node_names = None                   # List of node hostnames
node_ips = None                     # List of node IP addresses
cip_ips = None                      # List of node 10G addresses
ipmi_ips = None                     # List of node IPMI IP addresses
mip_netmask = None                  # Node 1G subnet mask
mip_gateway = None                  # Node 1G gateway
cip_netmask = None                  # Node 10G netmask
cip_gateway = None                  # Node 10G gateway
nameserver = None                   # DNS server
domain = None                       # DNS domain
vm_names = None                     # Virtual node VM names
min_iops = 100                      # QoS nin IOPS
max_iops = 100000                   # QoS max IOPS
burst_iops = 100000                 # QoS burst IOPS
fault_whitelist = [                 # Ignore these faults if they are present on the cluster
    "clusterFull",
    "clusterIOPSAreOverProvisioned",
    "nodeHardwareFault"
]
rtfi_options = "sf_auto=1,sf_secure_erase=0"
username = "admin"                  # Cluster admin username
password = "admin"                  # Cluster admin password
ssh_user = "root"                   # Cluster node SSH username
ssh_pass = "password"               # Cluster node ssh password
ipmi_user = "root"                  # IPMI username
ipmi_pass = "calvin"                # IPMI password

# Clients
client_ips = None                   # List of client IP addresses
client_user = "root"                # Client SSH username
client_pass = "password"            # Client SSH password
client_volume_sort = "iqn"          # Sort order for displaying client volumes

# Volumes
login_order = "serial"              # Order to log in to volumes on the client (serial or parallel)
auth_type = "chap"                  # iSCSI auth type - chap or none
connection_type = "iscsi"           # Type of volume connection (FC or iSCSI)
volume_access = "readWrite"         # Volume access level

# VDbench
vdbench_inputfile = "vdbench_input"         # Input file for vdbench
vdbench_outputdir = "output.tod"            # Output directory for vdbench
vdbench_workload = "rdpct=50,seekpct=random,xfersize=8k"    # IO workload to run
vdbench_data_errors = 5             # How many IO errors to fail vdbench after
vdbench_compratio = 2               # IO compression ratio
vdbench_dedupratio = 1              # IO dedup ratio
vdbench_run_time = "800h"           # How long to run IO
vdbench_interval = 10               # How often to report results
vdbench_threads = 4                 # Queue depth per device
vdbench_warmup = 0                  # How long to warmup before recording results
vdbench_data_vaidation = True       # Use data validation

# Infrastructure
nfs_ip = "192.168.154.7"            # The IP address of the main NFS datastore
nfs_mount_point = "/mnt/nfs"        # The mount point for the NFS datastore
email_notify = None                 # List of email addresses to send notifications to
email_from = "testscript@example.com"     # Email address to send notifications from
smtp_server = "aspmx.l.google.com"        # SMTP server for sending email
hipchat_user = "testscript"
hipchat_color = "gray"
pxe_server = None                   # PXE server to use to RTFI
pxe_user = None                     # PXE server username
pxe_pass = None                     # PXE server password
dns_servers = {                     # List of DNS servers for each site
    "ZDC" : {
        "nameserver" : "172.30.254.1",
        "domain" : "zdc.solidfire.net",
    },
    "VWC" : {
        "nameserver" : "172.26.254.1",
        "domain" : "eng.solidfire.net",
    },
    "BDR" : {
        "nameserver" : "172.24.254.1",
        "domain" : "eng.solidfire.net",
    },
    "DEN" : {
        "nameserver" : "10.117.30.11",
        "domain" : "one.den.solidfire.net",
    },
}
pxe_servers = {
    "BDR" : {
        "address" : "192.168.100.4",
        "username" : "root",
        "password" : "SolidF1r3",
    },
    "DEN" : {
        "address" : "10.117.30.30",
        "username" : "hciat2pxe",
        "password" : "6#&Pr#DuR?-uVX?1",
    }
}
# Virtualization
vmhost_user = "root"                # Hypervisor host username
vmhost_pass = "password"            # Hypervisor host password
vmhost_kvm = None                   # KVM hypervisor host
kvm_qcow2_name = "kvm-ubuntu-gold.qcow2"    # KVM template name
kvm_qcow2_path = nfs_mount_point + "/" + kvm_qcow2_name     # KVM qcow2 path
kvm_nfs_path = "/templates/kvm-templates"   # KVM template path on nfs
kvm_cpu_count = 1                   # KVM cpu count
kvm_mem_size = 512                  # KVM memory size
kvm_os = "linux"                    # KVM OS type
kvm_clone_name = "KVM-clone"        # KVM Clone name
kvm_network = "PrivateNet"          # KVM network bridge
kvm_connection = "tcp"              # KVM connection type
vmhost_xen = None                   # XenServer hypervisor host

vm_mgmt_server = None                   # Virtualization management server (vSphere for VMware, hypervisor for KVM)
vm_mgmt_user = None                     # VM management server username
vm_mgmt_pass = None                     # VM management server password
vmware_mgmt_server = "192.168.100.10"   # VMware vSphere management server
vmware_mgmt_user = "script"             # VMware username
vmware_mgmt_pass = "password"           # VMware password


# =============================================================================
# Timeouts

bin_sync_timeout = 3600             # Bin sync timeout, sec
slice_sync_timeout = 3600           # Slice sync timeout, sec
fill_timeout = 43200                # Cluster fill timeout, sec
gc_timeout = 90                     # How long to wait (min) for GC to finish
vlan_healthy_timeout = 300          # Seconds to wait for VLANs to be present and healthy on all cluster nodes
available_drives_timeout = 600      # Seconds to wait for available drives to show up
node_boot_timeout = 360             # Seconds for a node to boot up and be responding on the network

# =============================================================================
# Default Choices

all_output_formats = [
    "json",
    "bash"
]
all_volume_access_levels = [         # Valid volume access levels
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
    "iqn"
]
all_login_orders = [                # Valid login orders
    "serial",
    "parallel"
]
all_client_volume_sort = [          # Valid sort orders for client volumes
    "iqn",
    "device",
    "portal",
    "state",
    "sid"
]
all_drive_states = [
    "any",
    "active",
    "available",
    "failed",
    "removing"
]
all_node_states = [
    "pending",
    "active",
    "all"
]
all_compare_ops = {
    "eq" : "==",
    "ne" : "!=",
    "gt" : ">",
    "ge" : ">=",
    "lt" : "<",
    "le" : "<="
}
all_numbering_types = [
    'seq',
    'rev',
    'rand',
    'vol'
]
all_client_connection_types = [
    'fc',
    'iscsi'
]
all_network_config_options = [
    "keep",
    "clear",
    "reconfigure",
]
blacklisted_vm_names = [            # Names of VMs we are not allowed to operate on
    'jenkins',
    'artifacts'
]

# =============================================================================
# Times, mostly used so we can change them in UT
TIME_HOUR = 3600
TIME_MINUTE = 60
TIME_SECOND = 1

# =============================================================================
# Implementation details - do not modify anything below this line

__var_names = dir()

import os as __os
import re as __re
import sys as __sys
import inspect as __inspect
__thismodule = __sys.modules[__name__]

# Create an entry for each default with the original value
for __name in __var_names:
    __obj = getattr(__thismodule, __name)
    if __inspect.isbuiltin(__obj): continue
    if __inspect.ismodule(__obj): continue
    if __name.startswith("_"): continue

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
            setattr(__thismodule, __name, [s for s in __re.split(r"[\s,]+", __os.environ[__env_name]) if s])
        else:
            setattr(__thismodule, __name, __os.environ[__env_name])
del __env_name
del __name


def GetDefaults():
    """
    Get all of the defined default values

    Returns:
        A dictionary of default values
    """
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
    """
    Print all of the default values
    """
    defaults = GetDefaults()
    for name in sorted(defaults.keys()):
        print "%20s" % name + "  =>  " + str(defaults[name])
