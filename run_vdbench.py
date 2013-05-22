#!/usr/bin/python

"""
This action will run vdbench

When run as a script, the following options/env variables apply:
    --output_dir        The directory to put the vdbench out file into

    --input_file        The vdbench input file

    --additional_args   Any additional args to pass to vdbench on the command line

    --stdout_file       The name of the file to save vdbench stdout into
"""

import sys
from optparse import OptionParser
import subprocess
import os
import signal
import time
import re
import platform
import logging
import inspect
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libsf import ColorTerm
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class RunVdbenchAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_START_VDBENCH = "BEFORE_START_VDBENCH"
        AFTER_STOP_VDBENCH = "AFTER_STOP_VDBENCH"
        STARTING_RD = "STARTING_RD"
        VDBENCH_ERROR = "VDBENCH_ERROR"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)
        self._vdbench_process = None
        self.ReturnCode = -1

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"ouputDir" : None,
                            "inputFile" : None,
                            "stdoutFile" : None},
            args)

    def Execute(self, outputDir=sfdefaults.vdbench_outputdir, inputFile=sfdefaults.vdbench_inputfile, stdoutFile="bench.latest", debug=False):
        """
        Run vdbench
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        try:
            log = open(stdoutFile, "w")
            arg_list = ["/opt/vdbench/vdbench", "-o", outputDir, "-f ", inputFile]
            mylog.debug("Using command line " + " ".join(arg_list))
            log.write("Using command line " + " ".join(arg_list))
            self._RaiseEvent(self.Events.BEFORE_START_VDBENCH)

            self._vdbench_process = subprocess.Popen(arg_list, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in iter(self._vdbench_process.stdout.readline, ""):
                ColorTerm.screen.reset()
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.WhiteFore, ColorTerm.ConsoleColors.BlackBack)

                if (re.search("All slaves are now connected", line)):
                    ColorTerm.screen.set_color(ColorTerm.ConsoleColors.GreenFore, ColorTerm.ConsoleColors.BlackBack)
                elif (re.search("Starting RD", line)):
                    ColorTerm.screen.set_color(ColorTerm.ConsoleColors.GreenFore, ColorTerm.ConsoleColors.BlackBack)
                    self._RaiseEvent(self.Events.STARTING_RD)
                elif (re.search("Vdbench execution completed successfully", line)):
                    ColorTerm.screen.set_color(ColorTerm.ConsoleColors.GreenFore, ColorTerm.ConsoleColors.BlackBack)
                elif (re.search("No read validations done", line)):
                    ColorTerm.screen.set_color(ColorTerm.ConsoleColors.YellowFore, ColorTerm.ConsoleColors.BlackBack)
                    self.ReturnCode = 1
                elif (re.search("/var/log/messages", line)):
                    ColorTerm.screen.set_color(ColorTerm.ConsoleColors.YellowFore, ColorTerm.ConsoleColors.BlackBack)
                    self.ReturnCode = 2
                elif (re.search("conn error", line)):
                    ColorTerm.screen.set_color(ColorTerm.ConsoleColors.YellowFore, ColorTerm.ConsoleColors.BlackBack)
                    self.ReturnCode = 4
                    self._RaiseEvent(self.Events.VDBENCH_ERROR)
                elif (re.search("error", line, re.IGNORECASE)):
                    ColorTerm.screen.set_color(ColorTerm.ConsoleColors.RedFore, ColorTerm.ConsoleColors.BlackBack)
                    self.ReturnCode = 8
                    self._RaiseEvent(self.Events.VDBENCH_ERROR)
                elif (re.search("invalid", line, re.IGNORECASE)):
                    ColorTerm.screen.set_color(ColorTerm.ConsoleColors.RedFore, ColorTerm.ConsoleColors.BlackBack)
                    self.ReturnCode = 16
                    self._RaiseEvent(self.Events.VDBENCH_ERROR)
                elif (re.search("exception", line, re.IGNORECASE)):
                    ColorTerm.screen.set_color(ColorTerm.ConsoleColors.RedFore, ColorTerm.ConsoleColors.BlackBack)
                    self.ReturnCode = 32
                    self._RaiseEvent(self.Events.VDBENCH_ERROR)

                line = time.strftime("%Y-%m-%d ", time.localtime()) + line.rstrip()
                print line
                log.write(line + "\n")
                log.flush()

        except KeyboardInterrupt:
            self.Abort()

        log.close()
        ColorTerm.screen.reset()
        if (self.ReturnCode < 0):
            self.ReturnCode = self._vdbench_process.returncode

        if self.ReturnCode == 0:
            return True
        else:
            return False

    def Shutdown(self):
        self.Abort()
        sys.exit(0)

    def Abort(self):
        if self._vdbench_process:
            os.kill(self._vdbench_process.pid, signal.SIGINT)
        self.ReturnCode = 0
        ColorTerm.screen.reset()
        self._RaiseEvent(self.Events.AFTER_STOP_VDBENCH)

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--output_dir", type="string", dest="output_dir", default=sfdefaults.vdbench_outputdir, help="the output folder to pass to vdbench")
    parser.add_option("--input_file", type="string", dest="input_file", default=sfdefaults.vdbench_inputfile, help="the input file to pass to vdbench")
    parser.add_option("--sdout_file", type="string", dest="stdout_file", default="bench.latest", help="file to save vdbench stdout to")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    signal.signal(signal.SIGINT, Shutdown)
    signal.signal(signal.SIGTERM, Shutdown)
    if "windows" not in platform.system().lower():
        signal.signal(signal.SIGHUP, Shutdown)

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.output_dir, options.input_file, options.stdout_file, options.debug):
            sys.exit(0)
        else:
            # Use the vdbench return code as our return code
            sys.exit(action.ReturnCode)
    except libsf.SfArgumentError as e:
        mylog.error("Invalid arguments - \n" + str(e))
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        Abort()
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

