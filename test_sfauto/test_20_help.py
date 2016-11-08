#!/usr/bin/env python2.7
#pylint: skip-file

import os
import pytest
import subprocess


@pytest.mark.help
class TestHelp:

    def test_ShortHelp(self, scriptfiles_parametrize):
        """ Test the abbreviated help for each script """
    
        # Run the script with no args, expect argparse to fail on required arguments and print the short usage
        process = subprocess.Popen(scriptfiles_parametrize, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        retcode = process.returncode
    
        assert retcode == 2
        assert stdout == ''
        assert stderr.startswith("usage")

    def test_LongHelp(self, scriptfiles_parametrize):
        """ Test the full help for each script """
    
        # Run the script with long help arg, expect the help to be printed
        process = subprocess.Popen("'{}' --help".format(scriptfiles_parametrize), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        retcode = process.returncode

        # The script should exit 0, no stderr, and the usage in stdout
        assert retcode == 0
        assert stderr == ''
        assert stdout.startswith("usage")

        # help and debug options should always be present
        assert "-h, --help" in stdout
        assert "-d, --debug" in stdout

        # Run the script with the short help arg, expect the help to be the same as the long help
        process = subprocess.Popen("'{}' -h".format(scriptfiles_parametrize), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout2, stderr2 = process.communicate()
        retcode2 = process.returncode

        # The script should exit 0, no stderr, and the stdout should be the same as the long help
        assert retcode2 == 0
        assert stderr2 == ''
        assert stdout2 == stdout
