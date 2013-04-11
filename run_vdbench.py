#!/usr/bin/python

# This script will run vdbench

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

output_dir = '/var/log/testlogs/vdbench.tod'     # The folder to pass to vdbench to save the output files in
                                            # --output_dir

input_file = 'vdbench_input'                # The input file to pass to vdbench
                                            # --input_file

additional_args = ''                        # Any other arguments to pass to vdbench
                                            # --additional_args

output_file = 'bench.latest'                # File to save vdbench STDOUT to
                                            # --output_file

# ----------------------------------------------------------------------------


import sys, os
from optparse import OptionParser
import subprocess
import os
import signal
import time
import re
import datetime
import libsf
from libsf import mylog
from libsf import ColorTerm


def main():
    # Parse command line arguments
    parser = OptionParser()
    global output_dir, input_file, additional_args, output_file
    parser.add_option("--output_dir", type="string", dest="output_dir", default=output_dir, help="the output folder to pass to vdbench")
    parser.add_option("--input_file", type="string", dest="input_file", default=input_file, help="the input file to pass to vdbench")
    parser.add_option("--additional_args", type="string", dest="additional_args", default=additional_args, help="any other arguments to pass to vdbench")
    parser.add_option("--output_file", type="string", dest="output_file", default=output_file, help="file to save vdbench stdout to")

    (options, args) = parser.parse_args()
    output_dir = options.output_dir
    input_file = options.input_file
    additional_args = options.additional_args.split()
    output_file = options.output_file

    # Install signal handlers so we can run this script in the background
    import signal
    def shutdown_handler(signal, frame):
        global vdbench
        if vdbench:
            os.kill(vdbench.pid, signal.SIGINT)
        exit(1)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGHUP, shutdown_handler)

    returncode = -1
    try:
        log = open(output_file, "w")
        arg_list = ["/opt/vdbench/vdbench", "-o", output_dir, "-f ", input_file] + additional_args
        mylog.debug("Using command line " + " ".join(arg_list))
        log.write("Using command line " + " ".join(arg_list))

        global vdbench
        vdbench = subprocess.Popen(arg_list, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in iter(vdbench.stdout.readline, ""):
            ColorTerm.screen.reset()
            ColorTerm.screen.set_color(ColorTerm.ConsoleColors.WhiteFore, ColorTerm.ConsoleColors.BlackBack)

            if (re.search("All slaves are now connected", line)):
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.GreenFore, ColorTerm.ConsoleColors.BlackBack)
            elif (re.search("Starting RD", line)):
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.GreenFore, ColorTerm.ConsoleColors.BlackBack)
            elif (re.search("Vdbench execution completed successfully", line)):
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.GreenFore, ColorTerm.ConsoleColors.BlackBack)
            elif (re.search("No read validations done", line)):
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.YellowFore, ColorTerm.ConsoleColors.BlackBack)
                returncode = 1
            elif (re.search("/var/log/messages", line)):
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.YellowFore, ColorTerm.ConsoleColors.BlackBack)
                returncode = 2
            elif (re.search("conn error", line)):
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.YellowFore, ColorTerm.ConsoleColors.BlackBack)
                returnocde = 4
            elif (re.search("error", line, re.IGNORECASE)):
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.RedFore, ColorTerm.ConsoleColors.BlackBack)
                returncode = 8
            elif (re.search("invalid", line, re.IGNORECASE)):
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.RedFore, ColorTerm.ConsoleColors.BlackBack)
                returncode = 16
            elif (re.search("exception", line, re.IGNORECASE)):
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.RedFore, ColorTerm.ConsoleColors.BlackBack)
                returncode = 32

    #        if not (re.search("^\d{2}:\d{2}:\d{2}\.\d{3}", line)):
    #            line = time.strftime("%H:%M:%S    ", time.localtime()) + line

            line = time.strftime("%Y-%m-%d ", time.localtime()) + line.rstrip()
            print line
            log.write(line + "\n")
            log.flush()

    except KeyboardInterrupt:
        try: os.kill(vdbench.pid, signal.SIGINT)
        except OSError: pass

    log.close()
    ColorTerm.screen.reset()
    if (returncode < 0):
        returncode = vdbench.returncode

    exit(returncode)


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        #timer = libsf.ScriptTimer()
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)




