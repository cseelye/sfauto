#!/usr/bin/env python2.7
#pylint: skip-file

#import multiprocessing
from __future__ import print_function
import os
import pytest
import subprocess

def test_pylint_scripts(scriptfiles_parametrize):
    """ Run pylint on each script """

    command = "pylint --rcfile='{}' '{}'".format(os.path.join(pytest.sfauto_dir, "pylintrc"), scriptfiles_parametrize)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = process.communicate()
    retcode = process.returncode
    if retcode != 0 and stdout:
        print(stdout)

    # pylint return codes are a bitmap of:
    # 0 : no errors
    # 1 : fatal error in pylint itself
    # 2 : python error messages
    # 4 : python warning messages
    # 8 : python refactor messages
    # 16 : python conventions messages
    # 32 : usage error
    assert retcode == 0 or retcode == 16

def test_pylint_libs(libfiles_parametrize):
    """ Run pylint on each lib """

    command = "pylint --rcfile='{}' '{}'".format(os.path.join(pytest.sfauto_dir, "pylintrc"), libfiles_parametrize)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = process.communicate()
    retcode = process.returncode
    if retcode != 0 and stdout:
        print(stdout)

    # pylint return codes are a bitmap of:
    # 0 : no errors
    # 1 : fatal error in pylint itself
    # 2 : python error messages
    # 4 : python warning messages
    # 8 : python refactor messages
    # 16 : python conventions messages
    # 32 : usage error
    assert retcode == 0 or retcode == 16



