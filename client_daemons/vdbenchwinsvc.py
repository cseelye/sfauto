"""
Run vdbench as a Windows service

Install the service via 'python vdbenchwinsvc.py install'
Install the service via 'python vdbenchwinsvc.py remove'
Start/stop/configure the service in the standard Windows Services console

"""

import platform
import sys
if not platform.system().lower().startswith("win"):
    print "This script is strictly for Windows"
    sys.exit(0)

import datetime
import glob
import json
import os
import servicemanager
import signal
import subprocess
import time
import win32api
import win32con
import win32event
import win32service
import win32serviceutil
# Add my parent directory to the path, so I can find libs
sys.path.append(os.path.normpath(os.path.dirname(os.path.abspath(__file__)) + os.sep + ".."))
import lib.libsf as libsf

#class WindowsService(win32serviceutil.ServiceFramework):
#
#    def __init__(self, args):
#        win32serviceutil.ServiceFramework.__init__(self, args)
#        self.stopEvent = win32event.CreateEvent(None, 0, 0, None)
#
#    def SvcStop(self):
#        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
#        try:
#            self.stop()
#            win32event.SetEvent(self.stopEvent)
#        except Exception as ex:
#            servicemanager.LogErrorMsg(str(ex))
#
#    def SvcDoRun(self):
#        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
#        try:
#            self.start()
#            win32event.WaitForSingleObject(self.stopEvent, win32event.INFINITE)
#        except Exception as ex:
#            servicemanager.LogErrorMsg(str(ex))
#            self.SvcStop()
#
#    def start(self):
#        pass
#
#    def stop(self):
#        pass


class VdbenchWinSvc(win32serviceutil.ServiceFramework):
    _svc_name_ = "VDBenchSvc"
    _svc_display_name_ = "VDBench Service"
    _svc_description_ = "Run VDBench as a service in the background"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stopEvent = win32event.CreateEvent(None, 0, 0, None)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        try:
            self.stop()
            win32event.SetEvent(self.stopEvent)
        except Exception as ex:
            servicemanager.LogErrorMsg(str(ex))

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
        try:
            self.start()
            win32event.WaitForSingleObject(self.stopEvent, win32event.INFINITE)
        except Exception as ex:
            servicemanager.LogErrorMsg(str(ex))
            self.SvcStop()

    def start(self):
        self.abortFlag = False
        self.runningFlag = True
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
        servicemanager.LogInfoMsg("Starting VDBench service")

        try:
            hostname = platform.node()
            if "template" in hostname or "gold" in hostname:
                servicemanager.LogWarningMsg("Not running VDBench because my hostname looks like a template")
                self.ReportServiceStatus(win32service.SERVICE_RUNNING)
                return

            # Read config file
            self.vdbenchPath = r"C:\vdbench"
            self.parameterFile = r"C:\vdbench\parm"
            self.outputDirectory = r"c:\vdbench\output"
            try:
                config_file = os.path.splitext(__file__)[0] + ".json"
                with open(config_file, 'r') as f:
                    config_text = f.read()
                config = json.loads(config_text)
                self.vdbenchPath = config["vdbenchPath"]
                self.parameterFile = config["parameterFile"]
                self.outputDirectory = config["outputDirectory"]
            except:
                servicemanager.LogErrorMsg("Could not read config file " + config_file + " - using default values")

            while True:
                # Start vdbench
                self.vdbenchStartTime = datetime.datetime.now()
                self.argList = [self.vdbenchPath + r"\vdbench.bat", "-f", self.parameterFile, "-o", self.outputDirectory]
                servicemanager.LogInfoMsg("Starting VDBench with command line: " + " ".join(self.argList) + " in WD " + self.vdbenchPath)
                try:
                    self.vdbenchProcess = subprocess.Popen(self.argList, cwd=self.vdbenchPath)
                except OSError as e:
                    servicemanager.LogErrorMsg("Could not start vdbench: " + str(e))
                    self.ReportServiceStatus(win32service.SERVICE_STOPPED, win32ExitCode=win32service.SERVICE_SPECIFIC_ERROR, svcExitCode=1)

                self.ReportServiceStatus(win32service.SERVICE_RUNNING)
                servicemanager.LogInfoMsg("VDBench running with PID " + str(self.vdbenchProcess.pid))
                with open(self.vdbenchPath + r"\last_vdbench_pid", "w") as f:
                    f.write(str(self.vdbenchProcess.pid) + "\n")

                # Wait for vdbench to exit
                while self.vdbenchProcess.poll() is None:
                    time.sleep(5)

                # Check if we were requested to stop vs vdbench failed on its own
                if self.abortFlag:
                    servicemanager.LogInfoMsg("VDBench stopped")
                    # If we are being stopped, assume this is a gracefull shutdown and make the exit status reflect so
                    with open(self.vdbenchPath + r"\last_vdbench_exit", "w") as f:
                        f.write("0\n")
                    self.runningFlag = False
                    return

                # Check if vdbench failed
                if self.vdbenchProcess.returncode != 0:

                    # Look for known failures caused by clock skew and restart vdbench if we find one
                    restart_vdbench = False
                    with open(self.outputDirectory + r"\logfile.html", "r") as f:
                        line = f.readline()
                        if "start time greater than end time" in line:
                            restart_vdbench = True
                            break
                        if "Unable to find bucket" in line:
                            restart_vdbench = True
                            break
                    for fname in glob.glob(self.outputDirectory + r"\*.stdout.html"):
                        with open(fname, "r") as f:
                            line = f.readline()
                            if "Unable to find bucket" in line:
                                restart_vdbench = True
                                break

                    # Rename the output directory so it doesn't get overwritten
                    os.rename(self.outputDirectory, self.outputDirectory + "." + self.vdbenchStartTime.strftime("%Y-%m-%d-%H-%M-%S"))

                    if restart_vdbench:
                        continue

                    # Record the exit status
                    with open(self.vdbenchPath + r"\last_vdbench_exit", "w") as f:
                        f.write(str(self.vdbenchProcess.returncode) + "\n")

                    servicemanager.LogErrorMsg("VDBench failed with exit code " + str(self.vdbenchProcess.returncode))
                    self.runningFlag = False
                    self.ReportServiceStatus(win32service.SERVICE_STOPPED, win32ExitCode=win32service.SERVICE_SPECIFIC_ERROR, svcExitCode=self.vdbenchProcess.returncode)
                    return

                # Successful exit of vdbench
                with open(self.vdbenchPath + r"\last_vdbench_exit", "w") as f:
                    f.write("0\n")
                servicemanager.LogInfoMsg("VDBench finished with exit code 0")
                self.runningFlag = False
                return

        finally:
            self.runningFlag = False

    def stop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.abortFlag = True
        if self.vdbenchProcess.poll() is None:
            servicemanager.LogInfoMsg("Stopping VDBench")
            # Kill the process and all of its children.  E.g. if vdbench was launched as a shell process, this will kill cmd.exe, the vdb master, and vdb slave
            os.system("wmic Process WHERE ParentProcessID=" + str(self.vdbenchProcess.pid) + " delete 2>&1 > NUL")

        while self.runningFlag:
            time.sleep(0.5)




if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(VdbenchWinSvc)
