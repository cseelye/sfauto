#!/usr/bin/python

# This script will make sure your environment is sane and ready to run the scripts in this directory


try:
    import os
    import sys
except ImportError:
    print "ERROR: Can't find module 'os' or 'sys'.  Your python installation is really messed up."
    exit(1)

# Check for basic python modules
print "Testing for basic python modules"
modules = [
    "calendar",
    "colorconsole",
    "ctypes",
    "curses",
    "datetime",
    "email",
    "httplib",
    "inspect",
    "json",
    "logging",
    "os",
    "paramiko",
    "platform",
    "random",
    "re",
    "smtplib",
    "socket",
    "ssh",
    "string",
    "subprocess",
    "sys",
    "syslog",
    "threading",
    "time",
    "urllib2"
]
failed = False
for mod in modules:
    try:
        __import__(mod)
    except ImportError:
        print "  ERROR: Missing module '" + mod + "'"
        failed = True
if failed:
    exit(1)
else:
    print "  No errors"

# Make sure our own libs are available and logging is working
print "Testing for our basic libs and logging support"
try:
    import lib.libsf as libsf
    from lib.libsf import mylog
    import logging
    mylog.console.setLevel(logging.DEBUG)
    print "  You should see 7 log statements with different levels:"
    mylog.banner("Banner message")
    mylog.debug("Debug message")
    mylog.info("Informational message")
    mylog.warning("Warning message")
    mylog.error("Error message")
    mylog.passed("Passing message")
    mylog.step("Step message")
except ImportError as e:
    print "  ERROR: You appear to be missing lib/libsf.py or one of it's dependencies.  It's best to run these scripts straight out of where you clone the lab repo"
    print str(e)
    failed = True
try:
    import lib.libclient
    from lib.libclient import SfClient, ClientError
except ImportError as e:
    print "  ERROR: You appear to be missing lib/libclient.py or one of it's dependencies.  It's best to run these scripts straight out of where you clone the lab repo"
    print str(e)
    failed = True
if failed:
    exit(1)
else:
    print "  No errors"

# Look for optional components
print "Testing for optional components"

failed = False
if sys.platform.startswith("linux") and not os.path.exists("./winexe/bin/winexe.linux"):
    print "  WARNING: You appear to be missing winexe - you will not be able to control Windows clients"
    failed = True
if sys.platform.startswith("darwin") and not os.path.exists("./winexe/bin/winexe.linux"):
    print "  WARNING: You appear to be missing winexe - you will not be able to control Windows clients"
    failed = True
if not os.path.exists("./windiskhelper/diskapp.exe"):
    print "  WARNING: You appear to be missing diskapp - you will not be able to control Windows clients"
    failed = True
if not libsf.Which("smbclient"):
    print "  WARNING: You don't seem to have smbclient installed - you will not be able to control Windows clients"
    failed = True
if not libsf.Which("vdbench"):
    print "  WARNING: vdbench is not installed or not in your path.  You will not be able to run IO with clients"
    failed = True

if not failed:
    print "  No errors"

