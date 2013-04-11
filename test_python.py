
# This script will make sure your environment is sane and ready to run the scripts in this directory


# Check for basic python modules
print "Testing for basic python modules"
modules = [
    "ssh",
    "paramiko",
    "colorconsole",
    "sys",
    "ctypes",
    "platform",
    "time",
    "datetime",
    "calendar",
    "logging",
    "json",
    "urllib2",
    "httplib",
    "random",
    "socket",
    "syslog",
    "re",
    "os",
    "subprocess",
    "inspect",
    "curses",
    "threading",
    "string",
    "smtplib",
    "email"
]
failed = False
for mod in modules:
    try:
        __import__(mod)
    except ImportError:
        print "ERROR: Missing module '" + mod + "'"
        failed = True
if failed: sys.exit(1)

# Make sure our own libs are available and logging is working
print "Testing for our libs and logging support"
try:
    import libsf
    from libsf import mylog
    import logging
    mylog.console.setLevel(logging.DEBUG)
    print "You should see 5 log statements with different levels:"
    mylog.debug("Debug message")
    mylog.info("Informational message")
    mylog.warning("Warning message")
    mylog.error("Error message")
    mylog.passed("Passing message")
except ImportError:
    print "ERROR: You appear to be missing libsf.py or one of it's dependencies.  It's best to run these scripts straight out of where you clone the lab repo"
    failed = True
try:
    import libclient
    from libclient import SfClient, ClientError
except ImportError:
    print "ERROR: You appear to be missing libclient.py or one of it's dependencies.  It's best to run these scripts straight out of where you clone the lab repo"
    failed = True
if failed: sys.exit(1)

# Look for optional components
print "Testing for optional components"
try:
    import pysphere
except ImportError:
    print "WARNING: Missing module pysphere - you will not be able to run the vmware_* scripts."
try:
    import os
    import sys, os
except ImportError:
    print "ERROR: Can't find module 'os' or 'sys'.  Your python installation is really messed up."
else:
    if sys.platform.startswith("linux") and not os.path.exists("./winexe/bin/winexe.linux"):
        print "WARNING: You appear to be missing winexe - you will not be able to control Windows clients"
    if sys.platform.startswith("darwin") and not os.path.exists("./winexe/bin/winexe.linux"):
        print "WARNING: You appear to be missing winexe - you will not be able to control Windows clients"
    if not os.path.exists("./windiskhelper/diskapp.exe"):
        print "WARNING: You appear to be missing diskapp - you will not be able to control Windows clients"

if not libsf.Which("smbclient"):
    print "WARNING: You don't seem to have smbclient installed - you will not be able to control Windows clients"

if not libsf.Which("vdbench"):
    print "WARNING: vdbench is not installed or not in your path.  You will not be able to run IO with clients"



