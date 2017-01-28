#!/usr/bin/env python2.7
#pylint: skip-file

import glob
import logging
import multiprocessing
import os
import paramiko
import pytest
import time
import urllib2

from libsf import sfdefaults, shellutil, logutil
from .fake_client import FakeClientRegister, FakeShellCommand, FakeParamikoSSHClient
from .fake_cluster import FakeCluster, fake_urlopen
from . import globalconfig

# Add a command line option to specify the random seed to be used, so random tests can be repeated
def pytest_addoption(parser):
    parser.addoption("--seed", action="store", help="use the given seed instead of generating a new one")

# Configure all of the basics that all tests will need
def pytest_configure(config):

    # Make time pass quickly
    sfdefaults.TIME_MINUTE = 0
    sfdefaults.TIME_SECOND = 0
    sfdefaults.TIME_HOUR = 1

    # Turn up logging level
    logutil.GetLogger().ShowDebug(10)

    # Set the random seed
    user_seed = config.getoption("--seed")
    if user_seed:
        globalconfig.random_seed = user_seed
    else:
        globalconfig.random_seed = str(int(round(time.time() * 1000)))

    # Redirect urllib2.urlopen function so we can capture SF API calls (to simulate cluster/nodes)
    urllib2.urlopen = fake_urlopen

    # Redirect Shell function so we can capture commands like winexe, ping, etc (to simulate Windows clients, network operations, etc)
    shellutil.Shell_original = shellutil.Shell
    shellutil.Shell = FakeShellCommand

    # Redirect so we can capture SSH commands (to simulate Linux clients or SF nodes)
    paramiko.SSHClient = FakeParamikoSSHClient

# Teardown run after all tests are completed
def pytest_unconfigure(config):
    # Only keep the last 10 config files
    config_files = sorted([f for f in glob.glob("test/cluster-*") if os.path.isfile(f)], key=lambda x: os.path.getmtime(x))
    if len(config_files) > 10:
        for filename in config_files[10:]:
            os.unlink(filename)

# Add the seed that is being used into the header printed for the user to see
def pytest_report_header(config):
    return "\n  Using random seed: {}\n".format(globalconfig.random_seed)

# ===============================================================================================
# Initialize the fake cluster and fake clients for tests to use
def generate_fakes():
    sfdefaults.mvip = "9.9.9.9"
    globalconfig.clients = FakeClientRegister()
    globalconfig.cluster = FakeCluster()
    start = time.time()
    globalconfig.cluster.GenerateRandomConfig(globalconfig.random_seed)
    print "\nGenerated cluster in {} seconds".format(time.time() - start)

# Tear down the cluster so that it will be recreated in a pristine state
def fake_cluster_teardown():
    # Force the cluster to be regenerated
    globalconfig.cluster = None

# ===============================================================================================
# This fixture will provide a fake cluster and clients that is created at the beginning of the class and will maintain state
# throughout a single function but is torn down at the end of the class, before the next test is executed
@pytest.fixture(scope="class")
def fake_cluster_perclass(request):
    request.addfinalizer(fake_cluster_teardown)
    generate_fakes()
# ===============================================================================================


# ===============================================================================================
# This fixture will provide a fake cluster and clients that is created at the beginning of the function and will maintain state
# throughout a single function but is torn down at the end of the function, before the next test is executed
@pytest.fixture(scope="function")
def fake_cluster_permethod(request):
    request.addfinalizer(fake_cluster_teardown)
    generate_fakes()
# ===============================================================================================


# ===============================================================================================
@pytest.fixture(scope="class")
def fake_cluster_connected_clients(request):
    request.addfinalizer(fake_cluster_teardown)
    generate_fakes()
    
# ===============================================================================================


# ===============================================================================================
# Add a timer to each test
def timer_stop():
    print "\n{} seconds".format(time.time() - startTime)

startTime = 0
@pytest.fixture(scope="function", autouse=True)
def timer(request):
    global startTime
    startTime = time.time()
    request.addfinalizer(timer_stop)
# ===============================================================================================

def pytest_namespace():
    return {
        "sfauto_dir" : os.path.normpath(os.path.join(os.path.dirname(__file__), "..")),
        "sfauto_lib_dir" : os.path.normpath(os.path.join(os.path.dirname(__file__), "../libsf"))
    }

def pytest_generate_tests(metafunc):
    if "scriptfiles_parametrize" in metafunc.fixturenames:
        script_files = [os.path.join(pytest.sfauto_dir, f) for f in os.listdir(pytest.sfauto_dir) if os.path.isfile(os.path.join(pytest.sfauto_dir, f)) and f.endswith("py") and f != "test.py"]
        metafunc.parametrize("scriptfiles_parametrize", script_files)
    if "libfiles_parametrize" in metafunc.fixturenames:
        script_files = [os.path.join(pytest.sfauto_lib_dir, f) for f in os.listdir(pytest.sfauto_lib_dir) if os.path.isfile(os.path.join(pytest.sfauto_lib_dir, f)) and f.endswith("py")]
        metafunc.parametrize("libfiles_parametrize", script_files)


# ===============================================================================================
# Enable "incremental" test keyword, where if one test fails, the rest are skipped
# To mark a class as continuing incremental tests, use the decorator @pytest.mark.incremental
def pytest_runtest_makereport(item, call):
    if "incremental" in item.keywords:
        if call.excinfo is not None:
            parent = item.parent
            parent._previousfailed = item

def pytest_runtest_setup(item):
    if "incremental" in item.keywords:
        previousfailed = getattr(item.parent, "_previousfailed", None)
        if previousfailed is not None:
            pytest.xfail("previous test failed ({})".format(previousfailed.name))

# ===============================================================================================
