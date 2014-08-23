import libsf
from libsf import mylog
from pyVim import connect
from pyVmomi import vim, vmodl
import requests.exceptions

class VmwareError(Exception):
    def __init__(self, message, ex=None):
        self.message = message
        self.ex = ex
    def __str__(self):
        return self.message

class VsphereConnection(object):
    def __init__(self, server, username, password):
        self.server = server
        self.username = username
        self.password = password

    def __enter__(self):
        try:
            self.service = connect.SmartConnect(host=self.server, user=self.username, pwd=self.password)
        except vmodl.MethodFault as e:
            raise VmwareError("Could not connect: " + str(e), e)
        except vim.fault.InvalidLogin:
            raise VmwareError("Invalid credentials")
        except vim.fault.HostConnectFault as e:
            raise VmwareError("Could not connect: " + str(e), e)
        except requests.exceptions.ConnectionError as e:
            raise VmwareError("Could not connect: " + str(e), e)
        return self.service

    def __exit__(self, type, value, tb):
        mylog.debug("Disconnecting from vSphere " + self.server)
        connect.Disconnect(self.service)
