#!/usr/bin/env python
import logging
from logging.handlers import SysLogHandler
import ctypes
import sys
import platform
import time
import datetime
import calendar
import json
import urllib2
import BaseHTTPServer
import httplib
import random
import socket
if "win" not in platform.system().lower(): import syslog
import re
import os, subprocess
import commands
import inspect
import curses
import threading
import string
import traceback
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
try:
    import ssh
except ImportError:
    import paramiko as ssh

# Generic exception for all errors
class SfError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

# Exception for API errors
class SfApiError(SfError):
    def __init__(self, pErrorName, pErrorMessage):
        self.name = pErrorName
        self.message = pErrorMessage
    def __str__(self):
        return self.name + ": " + self.message

# Class for a generic collection of stuff, or as a dummy for compatability
class Bunch:
    __init__ = lambda self, **kw: setattr(self, '__dict__', kw)

# Custom log levels that map to specific colorized output
class MyLogLevels:
    PASS = 21
    RAW = 22
for attr, value in vars(MyLogLevels).iteritems():
    logging.addLevelName(attr, value)

# Cross platform colorizer for console logging
class ColorizingStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        logging.StreamHandler.__init__(self, stream)

    # color names to indices
    color_map = {
        'black': 0,
        'red': 1,
        'green': 2,
        'yellow': 3,
        'blue': 4,
        'magenta': 5,
        'cyan': 6,
        'white': 7,
    }

    #level to (background, foreground, bold/intense)
    level_map = {
        logging.DEBUG: ('black', 'white', False),
        logging.INFO: ('black', 'white', True),
        logging.WARNING: ('black', 'yellow', True),
        logging.ERROR: ('black', 'red', True),
        logging.CRITICAL: ('red', 'white', True),
        MyLogLevels.PASS: ('black', 'green', True),
        MyLogLevels.RAW: ('black', 'white', True),
    }
    csi = '\x1b['
    reset = '\x1b[0m'

    @property
    def is_tty(self):
        isatty = getattr(self.stream, 'isatty', None)
        return isatty and self.stream.isatty()

    def emit(self, record):
        try:
            message = self.format(record)
            stream = self.stream
            if not self.is_tty:
                stream.write(message)
            else:
                self.output_colorized(message)
            stream.write(getattr(self, 'terminator', '\n'))
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    if os.name != 'nt':
        def output_colorized(self, message):
            self.stream.write(message)
    else:
        import re
        ansi_esc = re.compile(r'\x1b\[((?:\d+)(?:;(?:\d+))*)m')

        nt_color_map = {
            0: 0x00,    # black
            1: 0x04,    # red
            2: 0x02,    # green
            3: 0x06,    # yellow
            4: 0x01,    # blue
            5: 0x05,    # magenta
            6: 0x03,    # cyan
            7: 0x07,    # white
        }

        def output_colorized(self, message):
            parts = self.ansi_esc.split(message)
            write = self.stream.write
            h = None
            fd = getattr(self.stream, 'fileno', None)
            if fd is not None:
                fd = fd()
                if fd in (1, 2): # stdout or stderr
                    h = ctypes.windll.kernel32.GetStdHandle(-10 - fd)
            while parts:
                text = parts.pop(0)
                if text:
                    write(text)
                if parts:
                    params = parts.pop(0)
                    if h is not None:
                        params = [int(p) for p in params.split(';')]
                        color = 0
                        for p in params:
                            if 40 <= p <= 47:
                                color |= self.nt_color_map[p - 40] << 4
                            elif 30 <= p <= 37:
                                color |= self.nt_color_map[p - 30]
                            elif p == 1:
                                color |= 0x08 # foreground intensity on
                            elif p == 0: # reset to default color
                                color = 0x07
                            else:
                                pass # error condition ignored
                        ctypes.windll.kernel32.SetConsoleTextAttribute(h, color)

    def colorize(self, message, record):
        if record.levelno in self.level_map:
            bg, fg, bold = self.level_map[record.levelno]
            params = []
            if bg in self.color_map:
                params.append(str(self.color_map[bg] + 40))
            if fg in self.color_map:
                params.append(str(self.color_map[fg] + 30))
            if bold:
                params.append('1')
            if params:
                message = ''.join((self.csi, ';'.join(params),
                                   'm', message, self.reset))
        return message

    def format(self, record):
        message = logging.StreamHandler.format(self, record)
        if self.is_tty:
            message = self.colorize(message, record)
        return message

# Custom formatter with different formats for different log levels
class MultiFormatter(logging.Formatter):
    raw_format = "%(message)s"
    std_format = "%(asctime)s: %(levelname)-7s %(message)s"

    def __init__(self, fmt=std_format):
        self.std_format = fmt
        logging.Formatter.__init__(self, fmt)
    def format(self, record):
        if record.levelno == MyLogLevels.RAW:
            self._fmt = self.raw_format
            result = logging.Formatter.format(self, record)
        else:
            self._fmt = self.std_format
            result = logging.Formatter.format(self, record)

        self._fmt = self.std_format
        return result

# Cross platform log to syslog and console with colors
class mylog:
    silence = False

    logging.raiseExceptions = False
    sftestlog = logging.getLogger("sftest")
    sftestlog.setLevel(logging.DEBUG)

    # Log everything to syslog on non-windows
    if "win" not in platform.system().lower():
        syslog_formatter = logging.Formatter("%(name)s: %(levelname)s %(message)s") # prepend with ident and our severities
        syslog_address = "/dev/log"
        if platform.system().lower() == "darwin": syslog_address="/var/run/syslog"
        syslog = SysLogHandler(address=syslog_address, facility=SysLogHandler.LOG_USER)
        syslog.setLevel(logging.DEBUG)
        syslog.setFormatter(syslog_formatter)
        sftestlog.addHandler(syslog)

    # Log info and above to screen with colors
    console_formatter = MultiFormatter('%(asctime)s: %(levelname)-7s %(message)s')
    console = ColorizingStreamHandler(stream=sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(console_formatter)
    sftestlog.addHandler(console)

    @staticmethod
    def _split_message(message):
        lines = []
        remain = str(message)
        while len(remain) > 1024:
            index = string.rfind(remain, " ", 0, 1024)
            if index <= 0:
                index = 1000
            lines.append(remain[:index])
            remain = remain[index:]
        lines.append(remain)
        return lines

    @staticmethod
    def debug(message):
        if mylog.silence: return
        lines = mylog._split_message(message)
        for line in lines:
            mylog.sftestlog.debug(line)
    @staticmethod
    def info(message):
        if mylog.silence: return
        lines = mylog._split_message(message)
        for line in lines:
            mylog.sftestlog.info(line)
    @staticmethod
    def warning(message):
        if mylog.silence: return
        lines = mylog._split_message(message)
        for line in lines:
            mylog.sftestlog.warning(line)
    @staticmethod
    def error(message):
        if mylog.silence: return
        lines = mylog._split_message(message)
        for line in lines:
            mylog.sftestlog.error(line)
    @staticmethod
    def exception(message):
        if mylog.silence: return
        lines = mylog._split_message(message)
        for line in lines:
            mylog.sftestlog.exception(line)
    @staticmethod
    def passed(message):
        if mylog.silence: return
        lines = mylog._split_message(message)
        for line in lines:
            mylog.sftestlog.log(MyLogLevels.PASS, line)

    @staticmethod
    def raw(message):
        if mylog.silence: return
        lines = mylog._split_message(message)
        for line in lines:
            mylog.sftestlog.log(MyLogLevels.RAW, line)

class ColorTerm:
    try:
        import termios
    except:
        termios = Bunch()
        termios.error = None

    class FallbackTerminal:
        def set_color(self, fore = None, back = None): pass
        def reset(self): pass
        def clear(self): pass

    # 'static constructor'
    screen = None
    try:
        import colorconsole
        from colorconsole import terminal
        screen = terminal.get_terminal()
    except termios.error:
        screen = FallbackTerminal()
    except ImportError:
        screen = FallbackTerminal()

    class ConsoleColors:
    # Background colors
        if (platform.system().lower() == 'windows'):
            CyanBack = 11
        else:
            CyanBack = 6
        if (platform.system().lower() == 'windows'):
            BlueBack = 9
        else:
            BlueBack = 4
        if (platform.system().lower() == 'windows'):
            RedBack = 4
        else:
            RedBack = 1
        if (platform.system().lower() == 'windows'):
            YellowBack = 14
        else:
            YellowBack = 3
        if (platform.system().lower() == 'windows'):
            PurpleBack = 11
        else:
            PurpleBack = 5
        LightGreyBack = 7
        GreenBack = 2
        BlackBack = 0
        PurpleBack = 5
    # Foreground colors
        BlackFore = 0
        PinkFore = 13
        PurpleFore = 5
        WhiteFore = 15
        GreenFore = 10
        LightGreyFore = 7
        if (platform.system().lower() == 'windows'):
            CyanFore = 11
        else:
            CyanFore = 14
        if (platform.system().lower() == 'windows'):
            RedFore = 12
        else:
            RedFore = 9
        if (platform.system().lower() == 'windows'):
            YellowFore = 14
        else:
            YellowFore = 11

class LocalTimezone(datetime.tzinfo):
    STDOFFSET = datetime.timedelta(seconds = -time.timezone)
    if time.daylight:
        DSTOFFSET = datetime.timedelta(seconds = -time.altzone)
    else:
        DSTOFFSET = LocalTimezone.STDOFFSET

    DSTDIFF = DSTOFFSET - STDOFFSET

    def utcoffset(self, dt):
        if self._isdst(dt):
            return LocalTimezone.DSTOFFSET
        else:
            return LocalTimezone.STDOFFSET

    def dst(self, dt):
        if self._isdst(dt):
            return LocalTimezone.DSTDIFF
        else:
            return datetime.timedelta(0)

    def tzname(self, dt):
        return time.tzname[self._isdst(dt)]

    def _isdst(self, dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = time.mktime(tt)
        tt = time.localtime(stamp)
        return tt.tm_isdst > 0

def ParseDateTime(pTimeString):
    known_formats = [
        "%Y-%m-%d %H:%M:%S.%f",     # old sf format
        "%Y-%m-%dT%H:%M:%S.%fZ",    # ISO format with UTC timezone
        "%Y-%m-%dT%H:%M:%SZ"     # almost ISO format with UTC timezone
                                    # 2012-11-15T19:18:46Z
    ]
    parsed = None
    for format in known_formats:
        try:
            parsed = datetime.datetime.strptime(pTimeString, format)
            break
        except ValueError: pass

    return parsed

def ParseTimestamp(pTimeString):
    date_obj = ParseDateTime(pTimeString)
    if (date_obj != None):
        return calendar.timegm(date_obj.timetuple())
    else:
        return 0

def ParseTimestampHiRes(pTimeString):
    date_obj = ParseDateTime(pTimeString)
    if (date_obj != None):
        return (calendar.timegm(date_obj.timetuple()) + date_obj.microsecond)
    else:
        return 0

def TimestampToStr(pTimestamp, pFormatString = "%Y-%m-%d %H:%M:%S", pTimeZone = LocalTimezone()):
    display_time = datetime.datetime.fromtimestamp(pTimestamp, pTimeZone)
    return display_time.strftime(pFormatString)

def CallNodeApiMethod(NodeIp, Username, Password, MethodName, MethodParams, ExitOnError=True, ApiVersion=5.0):
    rpc_url = 'https://' + NodeIp + ':442/json-rpc/' + ("%1.1f" % ApiVersion)
    password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(None, rpc_url, Username, Password)
    handler = urllib2.HTTPBasicAuthHandler(password_mgr)
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)

    api_call = json.dumps( { 'method': MethodName, 'params': MethodParams, 'id': random.randint(100, 1000) } )
    retry = 5
    last_error_code = ""
    last_error_mess = ""
    while(True):

        if retry <= 0:
            mylog.error("Could not call API method " + MethodName)
            if ExitOnError:
                sys.exit(1)
            else:
                raise SfApiError(last_error_code, last_error_message)

        # First try to get a valid HTTP reply to the web service call
        http_retry = 5
        while True:
            api_resp = None
            mylog.debug("Calling API on " + rpc_url + ": " + api_call)
            try:
                api_resp = urllib2.urlopen(rpc_url, api_call)
                break
            except urllib2.HTTPError as e:
                if (e.code == 401):
                    if ExitOnError:
                        mylog.error("Invalid username/password")
                        exit(1)
                    else:
                        raise SfApiError("HTTP error 404", "Invalid username/password")
                else:
                    last_error_code = str(e.code)
                    if (e.code in BaseHTTPServer.BaseHTTPRequestHandler.responses):
                        mylog.warning("HTTPError: " + str(e.code) + " " + str(BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code]))
                        last_error_message = str(BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code])
                    else:
                        mylog.warning("HTTPError: " + str(e.code))
            except urllib2.URLError as e:
                mylog.warning("URLError on " + rpc_url + " : " + str(e.reason))
                last_error_code = "URLError"
                last_error_mess = str(e.reason)
            except httplib.BadStatusLine as e:
                mylog.warning("httplib.BadStatusLine: " + str(e))
                last_error_code = "httplib.BadStatusLine"
                last_error_mess = str(e)
            except httplib.HTTPException as e:
                mylog.warning("HTTPException: " + str(e))
                last_error_code = "HTTPException"
                last_error_mess = str(e)

            http_retry -= 1
            if http_retry <= 0:
                if ExitOnError:
                    mylog.error("Could not call API method " + pMethodName)
                    exit(1)
                raise SfApiError(last_error_code, last_error_mess)

            mylog.info("Waiting 60 seconds before trying API again...")
            time.sleep(60)

        # At this point we got a good HTTP response code from the MVIP

        # Read the raw text content of the response
        response_str = api_resp.read().decode('ascii')
        #print "Raw response = ------------------------------------------------------"
        #print response_str
        #print "---------------------------------------------------------------------"

        # Make sure the response is the expected length
        expected_len = int(api_resp.headers['content-length'])
        actual_len = len(response_str)
        if (expected_len != actual_len):
            mylog.warning("API response: expected " + str(expected_len) + " bytes (content-length) but received " + str(actual_len) + " bytes")

        # Try to parse the response into JSON
        try:
            response_obj = json.loads(response_str)
        except ValueError:
            mylog.warning("Invalid JSON received from " + pMvip)
            last_error_code = "Unknown"
            last_error_mess = "Invalid JSON"
            if (not response_str.endswith("}")):
                mylog.warning("JSON appears truncated")
                last_error_mess = "Truncated JSON"
                retry -= 1
                continue # go back to the beginning and try to get a better response from the cluster

        # At this point we have a valid JSON object back from the cluster

        # If there was no error on the cluster side, return the response back to the caller
        if "error" not in response_obj:
            return response_obj["result"]

        # Record the error
        mylog.debug("Error response from " + str(pMvip) + ": " + str(response_str))
        last_error_code = response_obj["error"]["name"]
        last_error_mess = response_obj["error"]["message"]

        # See if it is an error we should retry
        if response_obj["error"]["name"] == "xDBConnectionLoss" and (pMethodName.startswith("List") or pMethodName.startswith("Get")):
            mylog.warning("Retrying because of xDBConnectionLoss")
            retry -= 1
            continue # go back to the beginning and try to get a better response from the cluster

        # Any other errors, fail
        if ExitOnError:
            mylog.error("Error " + response_obj['error']['name'] + " - " + response_obj['error']['message'])
            sys.exit(1)
        else:
            raise SfApiError(response_obj["error"]["name"], response_obj['error']['message'])

# Function for calling solidfire API methods
def CallApiMethod(pMvip, pUsername, pPassword, pMethodName, pMethodParams, ExitOnError=True, ApiVersion=1.0):
    rpc_url = 'https://' + pMvip + '/json-rpc/' + ("%1.1f" % ApiVersion)

    password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(None, rpc_url, pUsername, pPassword)
    handler = urllib2.HTTPBasicAuthHandler(password_mgr)
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)

    api_call = json.dumps( { 'method': pMethodName, 'params': pMethodParams, 'id': random.randint(100, 1000) } )
    retry = 5
    last_error_code = ""
    last_error_mess = ""
    while(True):

        if retry <= 0:
            mylog.error("Could not call API method " + pMethodName)
            if ExitOnError:
                sys.exit(1)
            else:
                raise SfApiError(last_error_code, last_error_message)

        # First try to get a valid HTTP reply to the web service call
        http_retry = 5
        while True:
            api_resp = None
            mylog.debug("Calling API on " + rpc_url + ": " + api_call)
            try:
                api_resp = urllib2.urlopen(rpc_url, api_call)
                break
            except urllib2.HTTPError as e:
                if (e.code == 401):
                    if ExitOnError:
                        mylog.error("Invalid username/password")
                        exit(1)
                    else:
                        raise SfApiError("HTTP error 404", "Invalid username/password")
                else:
                    last_error_code = str(e.code)
                    if (e.code in BaseHTTPServer.BaseHTTPRequestHandler.responses):
                        mylog.warning("HTTPError: " + str(e.code) + " " + str(BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code]))
                        last_error_message = str(BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code])
                    else:
                        mylog.warning("HTTPError: " + str(e.code))
            except urllib2.URLError as e:
                mylog.warning("URLError on " + rpc_url + " : " + str(e.reason))
                last_error_code = "URLError"
                last_error_mess = str(e.reason)
            except httplib.BadStatusLine as e:
                mylog.warning("httplib.BadStatusLine: " + str(e))
                last_error_code = "httplib.BadStatusLine"
                last_error_mess = str(e)
            except httplib.HTTPException as e:
                mylog.warning("HTTPException: " + str(e))
                last_error_code = "HTTPException"
                last_error_mess = str(e)

            http_retry -= 1
            if http_retry <= 0:
                if ExitOnError:
                    mylog.error("Could not call API method " + pMethodName)
                    exit(1)
                raise SfApiError(last_error_code, last_error_mess)

            mylog.info("Waiting 60 seconds before trying API again...")
            time.sleep(60)

        # At this point we got a good HTTP response code from the MVIP

        # Read the raw text content of the response
        response_str = api_resp.read().decode('ascii')
        #print "Raw response = ------------------------------------------------------"
        #print response_str
        #print "---------------------------------------------------------------------"

        # Make sure the response is the expected length
        expected_len = int(api_resp.headers['content-length'])
        actual_len = len(response_str)
        if (expected_len != actual_len):
            mylog.warning("API response: expected " + str(expected_len) + " bytes (content-length) but received " + str(actual_len) + " bytes")

        # Try to parse the response into JSON
        try:
            response_obj = json.loads(response_str)
        except ValueError:
            mylog.warning("Invalid JSON received from " + pMvip)
            last_error_code = "Unknown"
            last_error_mess = "Invalid JSON"
            if (not response_str.endswith("}")):
                mylog.warning("JSON appears truncated")
                last_error_mess = "Truncated JSON"
                retry -= 1
                continue # go back to the beginning and try to get a better response from the cluster

        # At this point we have a valid JSON object back from the cluster

        # If there was no error on the cluster side, return the response back to the caller
        if "error" not in response_obj:
            return response_obj["result"]

        # Record the error
        mylog.debug("Error response from " + str(pMvip) + ": " + str(response_str))
        last_error_code = response_obj["error"]["name"]
        last_error_mess = response_obj["error"]["message"]

        # See if it is an error we should retry
        if response_obj["error"]["name"] == "xDBConnectionLoss" and (pMethodName.startswith("List") or pMethodName.startswith("Get")):
            mylog.warning("Retrying because of xDBConnectionLoss")
            retry -= 1
            continue # go back to the beginning and try to get a better response from the cluster

        # Any other errors, fail
        if ExitOnError:
            mylog.error("Error " + response_obj['error']['name'] + " - " + response_obj['error']['message'])
            sys.exit(1)
        else:
            raise SfApiError(response_obj["error"]["name"], response_obj['error']['message'])

def ConnectSsh(pClientIp, pUsername, pPassword):
    client = ssh.SSHClient()
    client.load_system_host_keys()

    keyfile = None
    if sys.platform.startswith("win"):
        keyfile = os.environ["HOMEDRIVE"] + os.environ["HOMEPATH"] + "\\ssh\\id_rsa"
        if not os.path.exists(keyfile): keyfile = None
        else: mylog.debug("Connecting SSH to " + pClientIp + " using keyfile " + keyfile)

    client.set_missing_host_key_policy(ssh.AutoAddPolicy())
    try:
        client.connect(pClientIp, username=pUsername, password=pPassword, key_filename=keyfile)
    except ssh.AuthenticationException as e:
        # If a password was given, try again without the keyfile
        if keyfile and pPassword:
            try:
                mylog.debug("Connecting SSH to " + pClientIp + " as " + pUsername + "/" + pPassword)
                client.connect(pClientIp, username=pUsername, password=pPassword)
                return client
            except ssh.AuthenticationException: pass
        mylog.debug(str(e))
        mylog.error("Invalid username/password or key for " + pClientIp)
        exit(1)
    except ssh.SSHException as e:
        mylog.error("SSH error connecting to " + pClientIp + ": " + str(e))
        exit(1)
    except socket.error as e:
        mylog.error("Could not connect to " + pClientIp + ": " + str(e))
        exit(1)

    return client

def ExecSshCommand(pSshConnection, pCommand):
    hostname, port = pSshConnection._transport.getpeername()
    mylog.debug("Executing '" + pCommand + "' on host " + hostname)
    return pSshConnection.exec_command(pCommand)

class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None
        self.stdout = ''
        self.stderr = ''
        self.retcode = None

    def run(self, timeout):
        # Define a function to be run as a thread
        def process_thread():
            self.process = subprocess.Popen(self.cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            self.stdout, self.stderr = self.process.communicate()

        # Start the thread
        thread = threading.Thread(target=process_thread)
        thread.daemon = True
        thread.start()

        # Wait 'timeout' seconds for the thread to finish
        thread.join(timeout)

        # Kill the thread if it is still running
        if thread.is_alive():
            mylog.debug("Terminating subprocess '" + self.cmd + "' after " + str(timeout) + "sec")
            ppid = self.process.pid

            # The PID of the subprocess is actually the PID of the shell (/bin/sh or cmd.exe), since we launch with shell=True above.
            # This means that killing this PID leaves the actual process we are interested in running as an orphaned subprocess
            # So we need to kill all the children of that parent process
            if "win" in platform.system().lower():
                # This will kill everything in this shell as well as the shell itselfs
                os.system("wmic Process WHERE ParentProcessID=" + str(ppid) + " delete  2>&1 > NUL")
            else:
                # Under Linux you can simply do this, but MacOS does not have the --ppid flag:
                #os.system("for pid in $(ps --ppid " + str(ppid) + " -o pid --no-header); do kill -9 $pid; done")
                os.system("for pid in $(ps -eo ppid,pid | egrep \"^\\s*" + str(ppid) + "\\s+\" | awk '{print $2}'); do kill -9 $pid 2>&1 >/dev/null; done")

            # Now we can kill the parent process if it is still running and wait for the thread to finish
            try: self.process.kill()
            except WindowsError: pass
            thread.join()

        # Return the result of the command
        return self.process.returncode, self.stdout, self.stderr

def RunCommand(pCommandline, pTimeout=3600):
    command = Command(pCommandline)
    return command.run(pTimeout)

def Ping(pIpAddress):
    if (platform.system().lower() == 'windows'):
        command = "ping -n 2 %s"
    else:
        command = "ping -n -i 0.2 -c 5 -W 2 %s"
    ret = subprocess.call(command % pIpAddress, shell=True, stdout=open(os.devnull, 'w'), stderr=subprocess.STDOUT)
    if ret == 0:
        return True
    else:
        return False

def GetInterfaceList(pIpAddress, pUsername, pPassword):
    ssh = ConnectSsh(pIpAddress, pUsername, pPassword)
    stdin, stdout, stderr = ExecSshCommand(ssh, "ifconfig -a | grep eth")
    data = stdout.readlines()
    all_ifaces = []
    for line in data:
        m = re.search("^(eth\d+)\s+", line)
        if (m):
            iface = m.group(1)
            #mylog.info("  Found " + iface)
            all_ifaces.append(iface)
    ssh.close()
    return all_ifaces

def GetUpInterfaceList(pIpAddress, pUsername, pPassword):
    ssh = ConnectSsh(pIpAddress, pUsername, pPassword)
    stdin, stdout, stderr = ExecSshCommand(ssh, "ifconfig | grep eth")
    data = stdout.readlines()
    up_ifaces = []
    for line in data:
        m = re.search("^(eth\d+)\s+", line)
        if (m):
            iface = m.group(1)
            #mylog.info("  Found " + iface)
            up_ifaces.append(iface)
    ssh.close()
    return up_ifaces

def EnableInterfaces(pIpAddress, pUsername, pPassword):
    ssh = ConnectSsh(pIpAddress, pUsername, pPassword)
    all_ifaces = GetInterfaceList(pIpAddress, pUsername, pPassword)
    up_ifaces = GetUpInterfaceList(pIpAddress, pUsername, pPassword)
    down_ifaces = []
    for iface in all_ifaces:
        if iface not in up_ifaces:
            down_ifaces.append(iface)
            ExecSshCommand(ssh, "ifup " + iface)
    ssh.close()
    return down_ifaces

def ParseIpsFromList(pIpListString):
    if not pIpListString: return []
    #mylog.debug("Parsing " + pIpListString)

    ip_addr = []
    pieces = pIpListString.split(",")
    for ip in pieces:
        ip = ip.strip()
        #mylog.debug("Validating " + ip)
        if not IsValidIpv4Address(ip):
            raise TypeError("'" + ip + "' does not appear to be a valid address")
        ip_addr.append(ip)
    return ip_addr

def IsValidIpv4Address(pAddressString):
    if not pAddressString: return False
    pieces = pAddressString.split(".")
    last_octet = 0
    try:
        last_octet = int(pieces[-1])
    except ValueError: return False

    if len(pieces) != 4 or last_octet <= 0:
        return False
    try:
        addr = socket.inet_pton(socket.AF_INET, pAddressString)
    except AttributeError: # inet_pton not available
        try:
            addr = socket.inet_aton(pAddressString)
        except socket.error:
            return False
        pieces = pAddressString.split(".")
        return (len(pieces) == 4 and int(pieces[0]) > 0)
    except socket.error: # not a valid address
        return False

    return True

def GetSfVersion(pNodeIp):
    ssh = ConnectSsh(pNodeIp, "root", "password")
    command = "/sf/bin/sfapp --Version"
    stdin, stdout, stderr = ExecSshCommand(ssh, command)
    version = stdout.readlines()[0].strip()
    return version

def GetHostname(pIpAddress, pUsername, pPassword):
    ssh = ConnectSsh(pIpAddress, pUsername, pPassword)
    stdin, stdout, stderr = ExecSshCommand(ssh, "hostname")
    hostname = stdout.readlines()[0].strip()
    return hostname

def HttpRequest(pUrl, pUsername, pPassword):
    if (pUsername != None):
        mylog.debug("HTTP request to " + pUrl + " with credentials " + pUsername + ":" + pPassword)
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, pUrl, pUsername, pPassword)
        handler = urllib2.HTTPBasicAuthHandler(password_mgr)
        opener = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
    else:
        mylog.debug("HTTP request to " + pUrl)
    response = None
    try:
        response = urllib2.urlopen(pUrl)
    except KeyboardInterrupt: raise
    except: return None

    return response.read()

def SendEmail(pEmailTo, pEmailSubject, pEmailBody, pAttachments = None, pEmailFrom = None, pEmailServer = None, pServerUsername = None, pServerPassword = None):
    if (pEmailServer == "" or pEmailServer == None):
        pEmailServer = 'aspmx.l.google.com'
    if (pEmailFrom == "" or pEmailFrom == None):
        pEmailFrom = "testscript@nothing"

    if (type(pEmailTo) is list):
        send_to = pEmailTo
    else:
        send_to = []
        send_to.append(pEmailTo)

    if (type(pAttachments) is list):
        attachment_list = pAttachments
    elif (pAttachments == None):
        attachment_list = []
    else:
        attachment_list = []
        attachment_list.append(pAttachments)

    mylog.debug("Sending email to " + str(pEmailTo) + " via " + pEmailServer + ", Subject = " + pEmailSubject)

    msg = MIMEMultipart()
    msg['From'] = pEmailFrom
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = pEmailSubject

    msg.attach( MIMEText(pEmailBody) )

    for filename in attachment_list:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(filename,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(filename))
        msg.attach(part)

    smtp = smtplib.SMTP(pEmailServer)
    #smtp.connect()
    if (pServerUsername != None):
        smtp.starttls()
        smtp.login(pServerUsername,pServerPassword)
    smtp.sendmail(pEmailFrom, send_to, msg.as_string())
    smtp.close()

def IsSliceSyncing(pMvip, pUsername, pPassword):
    sync_html = HttpRequest("https://" + pMvip + "/reports/slicesyncing", pUsername, pPassword)
    if (sync_html != None and "table" in sync_html):
        return True
    else:
        return False

def IsBinSyncing(pMvip, pUsername, pPassword):
    sync_html = HttpRequest("https://" + pMvip + "/reports/binsyncing", pUsername, pPassword)
    if (sync_html != None and "table" in sync_html):
        return True
    else:
        return False

def GetLastGcInfo(pMvip, pUsername, pPassword):
    result = CallApiMethod(pMvip, pUsername, pPassword, 'ListServices', {})
    bs_count = 0
    if result is None: return None
    for service in result["services"]:
        service_info = service["service"]
        service_type = service_info["serviceType"]
        if (service_type == "block"): bs_count += 1

    event_list = CallApiMethod(pMvip, pUsername, pPassword, 'ListEvents', {})
    blocks_discarded = 0
    gc_generation = 0
    gc_complete_count = 0
    gc_start_time = 0
    gc_end_time = 0
    for i in range(len(event_list['events'])):
        event = event_list['events'][i]
        if ("GCStarted" in event["message"]):
            details = event["details"]
            m = re.search("GC generation:(\d+)", details)
            if (m):
                if (int(m.group(1)) == gc_generation):
                    gc_start_time = ParseTimestamp(event['timeOfReport'])
                    break
        if ("GCCompleted" in event["message"]):
            details = event["details"]
            pieces = details.split()
            if (gc_generation <= 0): gc_generation = int(pieces[0])
            if (int(pieces[0]) == gc_generation):
                gc_complete_count += 1
                blocks_discarded += int(pieces[1])
                end_time = ParseTimestamp(event['timeOfReport'])
                if (end_time > gc_end_time):
                    gc_end_time = end_time
    if (gc_complete_count >= bs_count):
        return (gc_start_time, gc_end_time, blocks_discarded * 4096)
    else:
        return (gc_start_time, 0, 0)

def CheckCoreFiles(pNodeIp, pUsername, pPassword, pSinceTime = 0):
    timestamp = TimestampToStr(pSinceTime, "%Y%m%d%H%M.%S")
    command = "touch -t " + timestamp + " /tmp/timestamp;find /sf -maxdepth 1 \\( -name \"core*\" ! -name \"core.zktreeutil*\" \\) -newer /tmp/timestamp| wc -l"
    #print command
    ssh = ConnectSsh(pNodeIp, pUsername, pPassword)
    stdin, stdout, stderr = ExecSshCommand(ssh, command)
    result = stdout.readlines()
    result = int(result[0].strip('\n'))
    return result

def CheckForEvent(pEventString, pMvip, pUsername, pPassword, pSinceTime = 0):
    event_list = CallApiMethod(pMvip, pUsername, pPassword, 'ListEvents', {})
    if event_list is None: return None
    for i in range(len(event_list['events'])):
        event = event_list['events'][i]
        if pEventString in event['message']:
            event_time = ParseTimestamp(event['timeOfReport'])
            if (event_time > pSinceTime):
                return True
    return False

def SecondsToElapsedStr(pSeconds):
    if type(pSeconds) is str: return pSeconds

    delta = datetime.timedelta(seconds=pSeconds)
    return TimeDeltaToStr(delta)

def TimeDeltaToStr(pTimeDelta):
    days = pTimeDelta.days
    hours = 0
    minutes = 0
    seconds = pTimeDelta.seconds
    if seconds >= 60:
        d,r = divmod(seconds, 60)
        minutes = d
        seconds = r
    if minutes >= 60:
        d,r = divmod(minutes, 60)
        hours = d
        minutes = r

    time_str = "%02d:%02d" % (minutes, seconds)
    if (hours > 0):
        time_str = "%02d:%02d:%02d" % (hours, minutes, seconds)
    if (days > 0):
        time_str = "%d-%02d:%02d:%02d" % (days, hours, minutes, seconds)

    return time_str

def WaitForBinSync(pMvip, pUsername, pPassword, pSince, pWaitThreshold = 0, pNotifyEmail = None):
    sync_start = 0
    mylog.info("Waiting for bin assignments change after " + TimestampToStr(pSince))
    while (sync_start <= 0):
        event_list = CallApiMethod(pMvip, pUsername, pPassword, 'ListEvents', {})
        for i in range(len(event_list['events'])-1, -1, -1):
            event = event_list['events'][i]
            if "Starting Bin Assignments Change" in event["message"]:
                #print "Found a sync start event at " + str(event["timeOfReport"])
                sync_start = ParseTimestamp(event['timeOfReport'])
                if (sync_start > pSince): break
                else: sync_start = 0
        if (sync_start <= 0): time.sleep(10)
    mylog.info("Bin sync started at " + TimestampToStr(sync_start))
    mylog.info("Waiting for final bin assignments completed")
    sync_finish = 0
    already_warned = False
    while (sync_finish <= 0):
        event_list = CallApiMethod(pMvip, pUsername, pPassword, 'ListEvents', {})
        for i in range(len(event_list['events'])-1, -1, -1):
            event = event_list['events'][i]
            if "Final Bin Assignments Completed" in event["message"]:
                sync_finish = ParseTimestamp(event['timeOfReport'])
                if (sync_finish >= sync_start): break
                else: sync_finish = 0
        # Check if sync is taking too long
        if pWaitThreshold > 0 and sync_finish <= 0 and (time.time() - sync_start > pWaitThreshold * 60) and not already_warned:
            mylog.warning("Bin syncing is taking longer than " + str(pWaitThreshold) + " min")
            already_warned = True
            if (pNotifyEmail != "" and pNotifyEmail != None):
                SendEmail(pNotifyEmail, "Long bin sync!", "Bin syncing is taking longer than " + str(pWaitThreshold) + " min")
        if (sync_finish <= 0): time.sleep(30)

    mylog.info("Bin sync completed at " + TimestampToStr(sync_finish))
    mylog.info("Bin sync duration " + SecondsToElapsedStr(sync_finish - sync_start))
    if pWaitThreshold > 0 and (sync_finish - sync_start > pWaitThreshold * 60):
        mylog.error("Sync took too long")
        return False
    return True

def HumanizeBytes(pBytes, pPrecision=1, pSuffix=None):
    if (pBytes == None):
        return "0 B"

    converted = float(pBytes)
    suffix_index = 0
    suffix = ['B', 'kiB', 'MiB', 'GiB', 'TiB']

    while (abs(converted) >= 1000):
        converted /= 1024.0
        suffix_index += 1
        if suffix[suffix_index] == pSuffix: break

    format_str = '%%0.%df %%s' % pPrecision
    return format_str % (converted, suffix[suffix_index])

def HumanizeDecimal(pNumber, pPrecision=1, pSuffix=None):
    if (pNumber == None):
        return "0"

    if (abs(pNumber) < 1000):
        return str(pNumber)

    converted = float(pNumber)
    suffix_index = 0
    suffix = [' ', 'k', 'M', 'G', 'T']

    while (abs(converted) >= 1000):
        converted /= 1000.0
        suffix_index += 1
        if suffix[suffix_index] == pSuffix: break

    format_str = '%%0.%df %%s' % pPrecision
    return format_str % (converted, suffix[suffix_index])

def CreateCenteredWindow(stdscr, windowHeight, windowWidth):
    term_height, term_width = stdscr.getmaxyx()
    hpad = (term_height - windowHeight) / 2
    wpad = (term_width - windowWidth) / 2
    return curses.newwin(windowHeight, windowWidth, hpad, wpad)

def WaitForGC(pMvip, pUsername, pPassword, pSince, pWaitThreshold = 30, pNotifyEmail = None):
    result = CallApiMethod(pMvip, pUsername, pPassword, 'ListServices', {})
    bs_count = 0
    if result is None: return None
    for service in result["services"]:
        service_info = service["service"]
        service_type = service_info["serviceType"]
        if (service_type == "block"): bs_count += 1

    mylog.info("Waiting for GC after " + TimestampToStr(pSince))

    blocks_discarded = 0
    gc_generation = 0
    gc_complete_count = 0
    gc_start_time = 0
    gc_end_time = 0

    # Find a GCStarted event after the 'since' time
    while(gc_start_time <= 0):
        event_list = CallApiMethod(pMvip, pUsername, pPassword, 'ListEvents', {})
        for i in range(len(event_list['events'])-1, -1, -1):
            event = event_list['events'][i]
            if ("GCRescheduled" in event["message"]):
                mylog.warning("GCRescheduled - GC was not run")
                return (ParseTimestamp(event['timeOfReport']), 0, 0)

            if ("GCStarted" in event["message"]):
                gc_start_time = ParseTimestamp(event['timeOfReport'])
                if (gc_start_time > pSince):
                    details = event["details"]
                    m = re.search("GC generation:(\d+)", details)
                    gc_generation = int(m.group(1))
                    break
                else: gc_start_time = 0
        if (gc_start_time <= 0): time.sleep(30)
    mylog.info("GC started at " + TimestampToStr(gc_start_time))
    mylog.info("Waiting for all BS to finish GC")

    # Need to modify this to catch GCRescheduled and consider that GC completion

    already_warned = False
    # Find GCCompleted event for each BS after the GCStarted time
    while(gc_complete_count < bs_count):
        event_list = CallApiMethod(pMvip, pUsername, pPassword, 'ListEvents', {})
        for i in range(len(event_list['events'])-1, -1, -1):
            event = event_list['events'][i]
            event_time = ParseTimestamp(event['timeOfReport'])
            if event_time < gc_start_time: continue

            if ("GCRescheduled" in event["message"]):
                mylog.warning("GCRescheduled - GC was not run")
                return (gc_start_time, 0, 0)

            if ("GCCompleted" in event["message"]):
                details = event["details"]
                pieces = details.split()
                if (int(pieces[0]) == gc_generation):
                    gc_complete_count += 1
                    blocks_discarded += int(pieces[1])
                    end_time = ParseTimestamp(event['timeOfReport'])
                    if (end_time > gc_end_time):
                        gc_end_time = end_time
        # Check if GC is taking too long
        if pWaitThreshold > 0 and gc_complete_count < bs_count and (time.time() - gc_start_time > pWaitThreshold * 60) and not already_warned:
            mylog.warning("GC is taking longer than " + str(pWaitThreshold) + " min")
            already_warned = True
            if (pNotifyEmail != "" and pNotifyEmail != None):
                SendEmail(pNotifyEmail, "Long bin sync!", "Bin syncing is taking longer than " + str(pWaitThreshold) + " min")
        if (gc_complete_count < bs_count): time.sleep(30)

    mylog.info("GC completed at " + TimestampToStr(gc_end_time))
    mylog.info("   Duration " + SecondsToElapsedStr(gc_end_time - gc_start_time))
    mylog.info("   Blocks discarded " + HumanizeDecimal(blocks_discarded*4095))

    return (gc_start_time, gc_end_time, blocks_discarded * 4096)

def MakeSimpleChapSecret(pLength=14):
    source_chars = string.ascii_letters + string.digits
    return "".join(random.choice(source_chars) for x in range(pLength))

def SearchForVolumes(pMvip, pUsername, pPassword, VolumeId=None, VolumeName=None, VolumeRegex=None, VolumePrefix=None, AccountName=None, VolumeCount=0):
    all_volumes = CallApiMethod(pMvip, pUsername, pPassword, "ListActiveVolumes", {})
    all_accounts = CallApiMethod(pMvip, pUsername, pPassword, "ListAccounts", {})

    # Find the source account if the user specified one
    source_account_id = 0
    account_volumes = dict()
    if AccountName:
        for account in all_accounts["accounts"]:
            if account["username"].lower() == AccountName.lower():
                source_account_id = account["accountID"]
                break
        if source_account_id <= 0:
            mylog.error("Could not find account '" + AccountName + "'")
            sys.exit(1)
        params = {}
        params["accountID"] = source_account_id
        account_volumes = CallApiMethod(pMvip, pUsername, pPassword, "ListVolumesForAccount", params)

    found_volumes = dict()
    count = 0

    # Search for specific volume id or list of ids
    if VolumeId:
        # Convert to a list if it is a scalar
        volume_id_list = []
        try:
            volume_id_list = list(VolumeId)
        except ValueError:
            volume_id_list.append(VolumeId)

        for vid in volume_id_list:
            volume_name = None
            for volume in all_volumes["volumes"]:
                if int(volume["volumeID"]) == vid:
                    volume_name = volume["name"]
                    break
            if volume_name == None:
                mylog.error("Could not find volume '" + str(vid) + "'")
                exit(1)
            found_volumes[volume_name] = vid

    # Search for a single volume name (or list of volume names) associated with a specific account
    # If there are duplicate volume names, the first match is taken
    elif VolumeName and AccountName:
        # Convert to a list if it is a scalar
        volume_name_list = []
        try:
            volume_name_list = list(VolumeName)
        except ValueError:
            volume_name_list.append(VolumeName)

        for vname in volume_name_list:
            volume_id = 0
            found = False
            for volume in account_volumes["volumes"]:
                if volume["name"] == vname:
                    if found:
                        mylog.warning("Duplicate volume name " + vname)
                        break
                    else:
                        volume_id = int(volume["volumeID"])
                        found = True
            if volume_id == None:
                mylog.error("Could not find volume '" + vname + "' on account '" + AccountName + "'")
                exit(1)
            found_volumes[vname] = volume_id

    # Search for a single volume name (or list of volume names) across all volumes.
    # If there are duplicate volume names, the first match is taken
    elif VolumeName:
        # Convert to a list if it is a scalar
        volume_name_list = []
        try:
            volume_name_list = list(VolumeName)
        except ValueError:
            volume_name_list.append(VolumeName)

        for vname in volume_name_list:
            volume_id = 0
            found = False
            for volume in all_volumes["volumes"]:
                if volume["name"] == vname:
                    if found:
                        mylog.warning("Duplicate volume name " + vname)
                        break
                    else:
                        volume_id = int(volume["volumeID"])
                        found = True
            if volume_id == None:
                mylog.error("Could not find volume '" + vname + "'")
                exit(1)
            found_volumes[vname] = volume_id

    # Search for regex match across volumes associated with a specific account
    elif VolumeRegex and AccountName:
        for volume in account_volumes["volumes"]:
            vol_id = int(volume["volumeID"])
            vol_name = volume["name"]
            m = re.search(VolumeRegex, vol_name)
            if m:
                if vol_name in found_volumes.keys():
                    mylog.warning("Duplicate volume name " + vol_name)
                    break
                else:
                    found_volumes[vol_name] = vol_id
                    count += 1
                    if VolumeCount > 0 and count >= VolumeCount: break

    # Search for regex match across all volumes
    elif VolumeRegex:
        for volume in all_volumes["volumes"]:
            vol_id = int(volume["volumeID"])
            vol_name = volume["name"]
            m = re.search(VolumeRegex, vol_name)
            if m:
                if vol_name in found_volumes.keys():
                    mylog.warning("Duplicate volume name " + vol_name)
                    break
                else:
                    found_volumes[vol_name] = vol_id
                    count += 1
                    if VolumeCount > 0 and count >= VolumeCount: break

    # Search for matching volumes on an account
    elif VolumePrefix and AccountName:
        for volume in account_volumes["volumes"]:
            if volume["name"].lower().startswith(VolumePrefix):
                if volume["name"] in found_volumes.keys():
                    mylog.warning("Duplicate volume name " + vol_name)
                    break
                else:
                    vol_id = int(volume["volumeID"])
                    vol_name = volume["name"]
                    found_volumes[vol_name] = vol_id
                    count += 1
                    if VolumeCount > 0 and count >= VolumeCount: break

    # Search for all matching volumes
    elif VolumePrefix:
        for volume in all_volumes["volumes"]:
            if volume["name"].lower().startswith(VolumePrefix):
                if volume["name"] in found_volumes.keys():
                    mylog.warning("Duplicate volume name " + vol_name)
                    break
                else:
                    vol_id = int(volume["volumeID"])
                    vol_name = volume["name"]
                    found_volumes[vol_name] = vol_id
                    count += 1
                    if VolumeCount > 0 and count >= VolumeCount: break

    # Search for all volumes on an account
    elif AccountName:
        for volume in account_volumes["volumes"]:
            vol_id = int(volume["volumeID"])
            vol_name = volume["name"]
            if vol_name in found_volumes.keys():
                mylog.warning("Duplicate volume name " + vol_name)
                break
            else:
                found_volumes[vol_name] = vol_id
                count += 1
                if VolumeCount > 0 and count >= VolumeCount: break

    return found_volumes

def Which(pExeName):
    for search_path in os.environ["PATH"].split(":"):
        if os.path.isdir(search_path) and pExeName in os.listdir(search_path):
            return os.path.join(search_path, pExeName)

def GetIpmiIp(NodeIp, Username, Password):
    ssh = None
    stdout_lines = None
    try:
        ssh = ConnectSsh(NodeIp, Username, Password)
        stdin, stdout, stderr = ExecSshCommand(ssh, "ipmitool lan print; echo $?")
        stdout_lines = stdout.readlines()
        if len(stdout_lines) <= 0:
            mylog.error("Failed to run ipmitool - " + "\n".join(stderr.readlines()))
            sys.exit(1)
        if (int(stdout_lines.pop().strip()) != 0):
            mylog.error("Failed to run ipmitool - " + "\n".join(stderr.readlines()))
            sys.exit(1)
    finally:
        if ssh:
            ssh.close()

    ipmi_ip = None
    for line in stdout_lines:
        m = re.search("IP Address\s+: (\S+)", line)
        if m:
            ipmi_ip = m.group(1)
            break
    if not ipmi_ip:
        mylog.error("Could not find an IPMI IP address for this node")
        sys.exit(1)

    return ipmi_ip

def IpmiCommand(IpmiIp, IpmiUsername, IpmiPassword, IpmiCommand):
    retry = 3
    status = None
    output = ""
    while retry > 0:
        status, output = commands.getstatusoutput("ipmitool -Ilanplus -U" + str(IpmiUsername) + " -P" + str(IpmiPassword) + " -H" + str(IpmiIp) + " -E " + str(IpmiCommand))
        if status == 0:
            break
        retry -= 1
        time.sleep(3)
