
import otp.CftGenericSSH as GenericSSH
import logging
import os
import sys
import shutil
import platform

def ExecSshCommand(IpAddress, Username, Password, Command):
    # Make sure the password files are in place
    if "win" in platform.system().lower():
        dest = os.path.expanduser("~/sftest")
    else:
        dest = os.path.expanduser("~/.sftest")
    if not os.path.exists(dest):
        os.makedirs(dest)
    shutil.copy("otp/be-otpw-words.txt", dest)
    shutil.copy("otp/b-otpw-words.txt", dest)

    # Silence logging from GenericSSH
    GenericSSH.log.setLevel(logging.FATAL)

    # Run the command
    stdout, stderr, return_code = GenericSSH.ssh_run_command(IpAddress, Command, username=Username, passwd=Password)

    # Get rid of log from GenericSSH, if it exists
    filename = os.path.splitext(os.path.basename(sys.modules['__main__'].__file__))[0]+'.log'
    if os.path.exists(filename):
        os.unlink(filename)

    return stdout, stderr, return_code

