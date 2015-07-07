#!/usr/bin/env python2.7
import logging
from logging.handlers import SysLogHandler
from logging.handlers import NTEventLogHandler
import optparse
from optparse import Option
import ctypes
import sys
import platform
import time
import datetime
import calendar
import json
import urllib2
import cookielib
import BaseHTTPServer
import httplib
import random
import socket
if "win" not in platform.system().lower(): import syslog
import multiprocessing
import multiprocessing.connection
from multiprocessing.connection import Listener
import re
import os, subprocess
import commands
import inspect
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
    import paramiko as ssh
except ImportError:
    import ssh
import shlex
import math
import struct

# Fix late-model python not working with self signed certs out of the box
try:
    import requests
    requests.packages.urllib3.disable_warnings()
except AttributeError:
    pass
try:
    import ssl
    #pylint: disable=protected-access
    ssl._create_default_https_context = ssl._create_unverified_context
    #pylint: enable=protected-access
except AttributeError:
    pass

class SfError(Exception):
    """
    Exception thrown when an error occurs
    """
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

class SfArgumentError(SfError):
    """
    Exception thrown when invalid arguments are passed
    """
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

class SfApiError(SfError):
    """
    Exception thrown when there is a SolidFire API error
    """
    def __init__(self, pErrorName, pErrorMessage):
        self.name = pErrorName
        self.message = pErrorMessage
    def __str__(self):
        return self.name + ": " + self.message

class SfTimeoutError(SfError):
    """
    Exception thrown when there is a timeout
    """

class SfUnknownObjectError(SfError):
    """
    Exception thrown when an object could not be found
    """

def ModuleNameToActionName(moduleName):
    action_name = ""
    pieces = moduleName.split("_")
    for piece in pieces:
        action_name += (piece[:1].upper() + piece[1:].lower())
    action_name += "Action"
    return action_name

def FileNameToActionName(fileName):
    action_name = ""
    fileName = os.path.basename(fileName)
    fileName, ext = os.path.splitext(fileName)
    pieces = fileName.split("_")
    for piece in pieces:
        action_name += (piece[:1].upper() + piece[1:].lower())
    action_name += "Action"
    return action_name

def PopulateActionModule(module):
    try:
        module.action = getattr(module, FileNameToActionName(module.__file__))()
    except AttributeError:
        mylog.error("Could not find action class for module " + module.__file__)
        sys.exit(1)
    for attr_name in dir(module.action):
        attr = getattr(module.action, attr_name)
        if inspect.isbuiltin(attr):
            continue
        if inspect.ismodule(attr):
            continue
        if attr_name.startswith("_"):
            continue
        setattr(module, attr_name, attr)

class ListOption(Option):
    """
    Option subclass for a comma delimited list of strings
    """
    ACTIONS = Option.ACTIONS + ("list",)
    STORE_ACTIONS = Option.STORE_ACTIONS + ("list",)
    TYPED_ACTIONS = Option.TYPED_ACTIONS + ("list",)
    ALWAYS_TYPED_ACTIONS = Option.ALWAYS_TYPED_ACTIONS + ("list",)

    def take_action(self, action, dest, opt, value, values, parser):
        if action == "list":
            # Split on any number of ',' or ' ' and remove empty entries
            lvalue = [i for i in re.split("[,\s]+", value) if i != None]
            if lvalue:
                setattr(values, dest, lvalue)
            if not values.ensure_value(dest, []):
                setattr(values, dest, [])
        else:
            Option.take_action(
                self, action, dest, opt, value, values, parser)

class ChildScript(object):
    """
    Class to run a script as a subprocess
    THe output of the script will be echoed to the screen
    To get results back, the script must use 0MQ IPC to send the result back
    """

    def __init__(self, command, timeout=600):
        try:
            import zmq
        except ImportError:
            raise SfError("You must install the 0MQ module to run this script")

        self.cmd = command
        self.timeout = timeout
        self.run_listener = True
        self.listener_ready = False
        self.result = None
        self.returncode = -1

    def _ResultListener(self):
        """
        Private thread to start a 0MQ listener to receive the result from the child script
        """
        import zmq
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.PAIR)
        self.zmq_port = self.zmq_socket.bind_to_random_port("tcp://*")
        mylog.debug("Listening for script result on port " + str(self.zmq_port))
        self.zmq_poller = zmq.Poller()
        self.zmq_poller.register(self.zmq_socket, zmq.POLLIN)
        self.listener_ready = True
        while self.run_listener:
            ready_sockets = dict(self.zmq_poller.poll(100))
            if self.zmq_socket in ready_sockets:
                msg = self.zmq_socket.recv()
                mylog.debug("Received result " + msg)
                try:
                    data = json.loads(msg)
                except ValueError as e:
                    mylog.error("Could not parse result from script: " + str(e))
                    return
                if 'result' in data:
                    self.result = data["result"]
                return

    def _ScriptRunner(self):
        """
        Private thread to run the child script and print the output to the screen
        """
        import zmq
        ColorTerm.screen.reset()
        cmd_list = shlex.split(self.cmd)
        cmd_list.append("--result_address=tcp://127.0.0.1:" + str(self.zmq_port))
        mylog.debug("Launching " + " ".join(cmd_list))
        self.process = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        for line in iter(self.process.stdout.readline, ""):
            if "DEBUG" in line:
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.LightGreyFore, ColorTerm.ConsoleColors.BlackBack)
            elif "INFO" in line:
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.WhiteFore, ColorTerm.ConsoleColors.BlackBack)
            elif "WARN" in line:
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.YellowFore, ColorTerm.ConsoleColors.BlackBack)
            elif "ERROR" in line:
                ColorTerm.screen.set_color(ColorTerm.ConsoleColors.RedFore, ColorTerm.ConsoleColors.BlackBack)

            print line.rstrip()
            ColorTerm.screen.reset()
        self.process.wait()
        self.returncode = self.process.returncode

    def Run(self):
        """
        Run the script and return the result
        """
        result_thread = threading.Thread(target=self._ResultListener, name="ResultListener")
        result_thread.daemon = True
        result_thread.start()
        while not self.listener_ready: continue

        script_thread = threading.Thread(target=self._ScriptRunner, name="ScriptRunner")
        script_thread.daemon = True
        script_thread.start()

        # Wait for 'timeout' seconds for the script to complete and kill it if it is still running
        script_thread.join(self.timeout)
        if script_thread.is_alive():
            mylog.debug("Killing child script " + self.cmd + " because it took too long")
            script_thread.terminate()

        # Shut down the result listener
        self.run_listener = False
        result_thread.join(5)
        if result_thread.is_alive():
            result_thread.terminate()

        return self.result

def kwargsToCommandlineArgs(kwargs):
    script_args = ""
    for arg_name in kwargs:
        if kwargs[arg_name] is False:
            continue
        if kwargs[arg_name] is True:
            script_args += "--" + arg_name
            continue
        script_args += " --" + arg_name + "=" + str(kwargs[arg_name])
    return script_args

# Class for a generic collection of stuff, or as a dummy for compatability
class Bunch:
    __init__ = lambda self, **kw: setattr(self, '__dict__', kw)

# Mimic an enumerated type
def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

# Custom log levels that map to specific colorized output
class MyLogLevels:
    PASS = 21
    RAW = 22
    TIME = 23
    BANNER = 24
    STEP = 25
    EXCEPTION = 26
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
        MyLogLevels.PASS: ('black', 'green', True),
        MyLogLevels.RAW: ('black', 'white', True),
        MyLogLevels.TIME: ('black', 'cyan', False),
        MyLogLevels.BANNER: ('black', 'magenta', True),
        MyLogLevels.STEP: ('black', 'cyan', False),
        MyLogLevels.EXCEPTION: ('black', 'red', True),
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
            import win32console
            fn = getattr(self.stream, 'fileno', None)
            if fn is not None:
                fd = fn()
                if fd in (1, 2): # stdout or stderr
                    c = win32console.GetStdHandle(-10 - fd)
            parts = self.ansi_esc.split(message)
            while parts:
                text = parts.pop(0)
                if text:
                    self.stream.write(text)
                if parts:
                    params = parts.pop(0)
                    if c is not None:
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
                                pass # unknown color command - ignore it

                        c.SetConsoleTextAttribute(color)

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
    try:
        #try and get the width of the terminal shell
        rows, columns = os.popen('stty size', 'r').read().split()
        banner_width = int(columns)
    except:
        banner_width = 120
    raw_format = "%(message)s"
    std_format = "%(asctime)s: %(levelname)-7s %(message)s"
    banner_format = "="*banner_width + "\n%(message)s\n" + "="*banner_width

    def __init__(self, fmt=std_format):
        self.std_format = fmt
        logging.Formatter.__init__(self, fmt)
    def format(self, record):
        if record.levelno == MyLogLevels.RAW or record.levelno == MyLogLevels.TIME:
            self._fmt = self.raw_format
        elif record.levelno == MyLogLevels.BANNER:
            # Center the message and make sure it fits within the banner
            modified = []
            for line in record.msg.split("\n"):
                if len(line) > self.banner_width:
                    pieces = SplitMessage(line, self.banner_width)
                else:
                    pieces = [line]
                for piece in pieces:
                    modified.append(piece.center(self.banner_width, ' '))
            record.msg = "\n".join(modified)
            self._fmt = self.banner_format
        elif record.levelno == MyLogLevels.STEP:
            record.msg = ">>> " + record.msg
        elif record.levelno == MyLogLevels.EXCEPTION:
            record.msg = record.msg + "\n{}".format(traceback.format_exc())
        else:
            self._fmt = self.std_format

        result = logging.Formatter.format(self, record)

        self._fmt = self.std_format
        return result

class mylog:
    """ Cross platform log to syslog/console with colors"""

    silence = False

    logging.raiseExceptions = False
    sftestlog = logging.getLogger("sftest")
    sftestlog.setLevel(logging.DEBUG)

    # Log everything to the platform appropriate syslog
    if platform.system().lower().startswith("win"):
        import pywintypes
        try:
            eventlog_formatter = logging.Formatter("%(levelname)s %(message)s") # prepend with ident and our severities
            eventlog = NTEventLogHandler("sftest")
            eventlog.setLevel(logging.DEBUG)
            eventlog.setFormatter(eventlog_formatter)
            sftestlog.addHandler(eventlog)
        except pywintypes.error:
            # Probably not running as administrator
            pass
    else:
        from logging.handlers import SysLogHandler
        syslog_formatter = logging.Formatter("%(name)s: %(levelname)s %(message)s") # prepend with ident and our severities
        syslog = None
        # Try to connect to syslog on the local unix socket
        syslog_address = "/dev/log"
        if "darwin" in platform.system().lower():
            syslog_address="/var/run/syslog"
        try:
            syslog = SysLogHandler(address=syslog_address, facility=SysLogHandler.LOG_USER)
        except socket.error:
            # Try again with UDP
            syslog = SysLogHandler(address=('localhost', 514), facility=SysLogHandler.LOG_USER)

        if syslog:
            syslog.setLevel(logging.DEBUG)
            syslog.setFormatter(syslog_formatter)
            sftestlog.addHandler(syslog)

    # Log info and above to screen with colors
    console_formatter = MultiFormatter('%(asctime)s: %(levelname)-7s %(message)s')
    console = ColorizingStreamHandler(stream=sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(console_formatter)
    sftestlog.addHandler(console)

    class loggermethod(object):
        """Decorator to create a logger method
        Must be used as a function even when not passing arguments, ie @loggermethod()
        """
        def __init__(self, severity=None):
            if hasattr(severity, "__call__"):
                raise Exception("{} decorator must be called as a function".format(self.__class__.__name__))
            else:
                self.severity = severity

        def __call__(self, func):
            if not self.severity:
                self.severity = func.__name__
            severity = self.severity.upper()

            try:
                level = logging._checkLevel(severity)
            except ValueError:
                raise Exception("Log level '{}' is not defined".format(self.severity))

            def logfunc(message):
                if mylog.silence:
                    return
                lines = SplitMessage(message)
                for line in lines:
                    mylog.sftestlog.log(level, line)

            # Set some metadata on this method
            setattr(logfunc, "isLoggerFunction", True)
            setattr(logfunc, "severity", func.__name__)
            setattr(logfunc, "original", func)

            return logfunc

    @staticmethod
    def GetLoggerMethods():
        methods=[]
        for name, func in inspect.getmembers(mylog, predicate=inspect.isroutine):
            if getattr(func, "isLoggerFunction", False):
                methods.append(func)
        return methods

    @staticmethod
    def showDebug():
        mylog.console.setLevel(logging.DEBUG)

    @staticmethod
    def hideDebug():
        mylog.console.setLevel(logging.INFO)

    @staticmethod
    @loggermethod()
    def debug(message): pass

    @staticmethod
    @loggermethod()
    def info(message): pass

    @staticmethod
    @loggermethod()
    def warning(message): pass

    @staticmethod
    @loggermethod()
    def error(message): pass

    @staticmethod
    @loggermethod()
    def exception(message): pass

    @staticmethod
    @loggermethod("pass")
    def passed(message): pass

    @staticmethod
    @loggermethod()
    def time(message): pass

    @staticmethod
    @loggermethod()
    def raw(message): pass

    @staticmethod
    @loggermethod()
    def banner(message): pass

    @staticmethod
    @loggermethod()
    def step(message): pass


def SplitMessage(message, length=1024):
    lines = []
    remain = str(message)
    while len(remain) > length:
        index = string.rfind(remain, " ", 0, length)
        if index <= 0:
            index = length - 1
        lines.append(remain[:index])
        remain = remain[index:]
    lines.append(remain)
    return lines

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

    def __init__(self):

        self.STDOFFSET = datetime.timedelta(seconds = -time.timezone)
        if time.daylight:
            self.DSTOFFSET = datetime.timedelta(seconds = -time.altzone)
        else:
            self.DSTOFFSET = self.STDOFFSET

        self.DSTDIFF = self.DSTOFFSET - self.STDOFFSET

    def utcoffset(self, dt):
        if self._isdst(dt):
            return self.DSTOFFSET
        else:
            return self.STDOFFSET

    def dst(self, dt):
        if self._isdst(dt):
            return self.DSTDIFF
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

class ScriptTimer:
    def __init__(self):
        self.name = os.path.basename(inspect.stack()[-1][1])
        self.startTime = time.time()
    def __del__(self):
        endTime = time.time()
        mylog.time(self.name + " total run time " + SecondsToElapsedStr(endTime - self.startTime))

class SyncCounter(object):
    """
    Thread safe counter
    """
    def __init__(self, initialValue=0):
        self.count = multiprocessing.RawValue("i", initialValue)
        self.lock = multiprocessing.Lock()

    def Increment(self):
        with self.lock:
            self.count.value += 1

    def Decrement(self):
        with self.lock:
            self.count.value += 1

    def Value(self):
        with self.lock:
            return self.count.value

def ParseDateTime(pTimeString):
    known_formats = [
        "%Y-%m-%d %H:%M:%S.%f",     # old sf format
        "%Y-%m-%dT%H:%M:%S.%fZ",    # ISO format with UTC timezone
        "%Y-%m-%dT%H:%M:%SZ",     # almost ISO format with UTC timezone
        "%Y%m%dT%H:%M:%SZ"
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
        timestamp = calendar.timegm(date_obj.timetuple())
        if timestamp <= 0:
            timestamp = calendar.timegm(date_obj.utctimetuple())
        return timestamp
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

def CallNodeApiMethod(NodeIp, Username, Password, MethodName, MethodParams, ExitOnError=False, ApiVersion=5.0):
    rpc_url = 'https://' + NodeIp + ':442/json-rpc/' + ("%1.1f" % ApiVersion)
    return __CallApiMethodCommon(NodeIp, rpc_url, Username, Password, MethodName, MethodParams, ExitOnError, ApiVersion)

cookie_jar = None
def __CallApiMethodCommon(Ip, Url, Username, Password, MethodName, MethodParams, ExitOnError=False, ApiVersion=5.0, UseCookies=False):
    if UseCookies:
        global cookie_jar
        if not cookie_jar:
            cookie_jar = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie_jar))
    else:
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        if Username:
            password_mgr.add_password(None, Url, Username, Password)
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
                raise SfApiError(last_error_code, last_error_mess)

        # First try to get a valid HTTP reply to the web service call
        http_retry = 5
        while True:
            api_resp = None
            if Username:
                mylog.debug("Calling API on " + Url + ": " + api_call + " as " + str(Username) + " : " + str(Password))
            else:
                mylog.debug("Calling API on " + Url + ": " + api_call)
            try:
                api_resp = urllib2.urlopen(Url, api_call)
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
                        mylog.warning("HTTPError: " + str(e.code) + " " + str(BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code]) + " " + Url)
                        last_error_mess = str(BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code])
                    else:
                        mylog.warning("HTTPError: " + str(e.code)  + " " + Url)
            except urllib2.URLError as e:
                # Immediately fail for "network unreachable", "operation timed out", "connection refused"
                if e.args and (e.args[0].errno == 51 or e.args[0].errno == 60 or e.args[0].errno == 111):
                    if ExitOnError:
                        mylog.error("Could not call API method " + MethodName + ": " + e.args[0].strerror)
                    else:
                        raise SfApiError("URLError", e.args[0].strerror)
                mylog.warning("URLError on " + Url + " : " + str(e.reason))
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
            except socket.errno as e:
                mylog.warning("Socket error: " + str(e))
                last_error_code = "SocketError"
                last_error_mess = str(e)

            http_retry -= 1
            if http_retry <= 0:
                if ExitOnError:
                    mylog.error("Could not call API method " + MethodName)
                    exit(1)
                raise SfApiError(last_error_code, last_error_mess)

            mylog.info("Waiting 30 seconds before trying API again...")
            time.sleep(30)

        # At this point we got a good HTTP response code from the URL

        # Read the raw text content of the response
        response_str = api_resp.read().decode('ascii')
        #print "Raw response = ------------------------------------------------------"
        #print response_str
        #print "---------------------------------------------------------------------"

        # Make sure the response is the expected length
        if 'content-length' in api_resp.headers:
            expected_len = int(api_resp.headers['content-length'])
            actual_len = len(response_str)
            if (expected_len != actual_len):
                mylog.warning("API response: expected " + str(expected_len) + " bytes (content-length) but received " + str(actual_len) + " bytes")

        # Try to parse the response into JSON
        try:
            response_obj = json.loads(response_str)
        except ValueError:
            mylog.warning("Invalid JSON received from " + Ip)
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
        mylog.debug("Error response from " + str(Ip) + ": " + str(response_str))
        last_error_code = response_obj["error"]["name"]
        last_error_mess = response_obj["error"]["message"]

        # See if it is an error we should retry
        #if response_obj["error"]["name"] == "xDBConnectionLoss" and (MethodName.startswith("List") or MethodName.startswith("Get")):
        if response_obj["error"]["name"] == "xDBConnectionLoss":
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
def CallApiMethod(Mvip, Username, Password, MethodName, MethodParams, ExitOnError=False, ApiVersion=5.0, UseCookies=False):
    rpc_url = 'https://' + Mvip + '/json-rpc/' + ("%1.1f" % ApiVersion)
    return __CallApiMethodCommon(Mvip, rpc_url, Username, Password, MethodName, MethodParams, ExitOnError, ApiVersion, UseCookies)

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

        raise SfError("Invalid username/password or key for " + pClientIp)
    except ssh.SSHException as e:
        raise SfError("SSH error connecting to " + pClientIp + ": " + str(e))
    except socket.error as e:
        raise SfError("Could not connect to " + pClientIp + ": " + str(e))

    return client

def ExecSshCommand(sshConnection, command, timeout=300):
    hostname, port = sshConnection._transport.getpeername()
    mylog.debug("Executing '" + command + "' on host " + hostname)
    #return sshConnection.exec_command(command)
    chan = sshConnection._transport.open_session()
    chan.settimeout(timeout)
    chan.exec_command(command)
    stdin = chan.makefile('wb', -1)
    stdout = chan.makefile('rb', -1)
    stderr = chan.makefile_stderr('rb', -1)
    return stdin, stdout, stderr

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
            if "windows" in platform.system().lower():
                # This will kill everything in this shell as well as the shell itself
                os.system("wmic Process WHERE ParentProcessID=" + str(ppid) + " delete 2>&1 > NUL")
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
    elif "darwin" in platform.system().lower():
        command = "ping -n -i 1 -c 3 -W 2 %s"
    else:
        command = "ping -n -i 0.2 -c 5 -W 2 %s"
    ret = subprocess.call(command % pIpAddress, shell=True, stdout=open(os.devnull, 'w'), stderr=subprocess.STDOUT)
    if ret == 0:
        return True
    else:
        return False

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
    if not pAddressString:
        return False
    elif any (c.isalpha() for c in pAddressString):
        try:
            tempAddressString = pAddressString
            pAddressString = socket.gethostbyname(tempAddressString)
        except socket.gaierror as e: #Unable to resolve host name
            mylog.error(" invalid HostName: " + str(e) )
            return False
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

def IsValidIpv4AddressList(addressList):
    if not addressList:
        return False
    for ip in addressList:
        if not IsValidIpv4Address(ip):
            return False
    return True

def isValidMACAddress(macAddress):
    """
    Checks to make sure something is a valid mac address
    """
    if re.match("[0-9a-f]{2}([-:])[0-9a-f]{2}(\\1[0-9a-f]{2}){4}$", macAddress.lower()):
        return True
    return False

def ParseIntsFromList(integerListString):
    if not integerListString: return []
    int_list = []
    pieces = integerListString.split(",")
    for i in pieces:
        i = i.strip()
        try:
            i = int(i)
        except ValueError:
            raise TypeError("'" + str(i) + "' is not a valid integer")
        int_list.append(i)
    return int_list

def IsInteger(valueToTest):
    try:
        int(valueToTest)
    except ValueError:
        return False
    return True

def IsPositiveInteger(valueToTest):
    return IsInteger(valueToTest) and valueToTest >= 0

def IsPositiveNonZeroInteger(valueToTest):
    return IsInteger(valueToTest) and valueToTest > 0

def IsIntegerList(valueToTest):
    for i in valueToTest:
        if not IsInteger(i):
            return False
    return True

def GetFirstLine(stringValue):
    if not stringValue:
        return stringValue

    lines = str(stringValue).split("\n")
    for line in lines:
        if not line:
            continue
        return line
    return stringValue

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
        pEmailFrom = "testscript@solidfire.com"

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
    import curses
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

    mylog.info("Waiting for a GC after " + TimestampToStr(pSince) + " to complete on " + str(bs_count) + " BServices")

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
            #if ("GCRescheduled" in event["message"]):
            #    event_time = ParseTimestamp(event['timeOfReport'])
            #    if event_time < pSince: continue
            #    mylog.warning("GCRescheduled - GC was not run")
            #    return (ParseTimestamp(event['timeOfReport']), 0, 0)

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

def SearchForVolumes(pMvip, pUsername, pPassword, VolumeId=None, VolumeName=None, VolumeRegex=None, VolumePrefix=None, AccountName=None, AccountId=None, VolumeCount=0):
    all_volumes = CallApiMethod(pMvip, pUsername, pPassword, "ListActiveVolumes", {})
    all_accounts = CallApiMethod(pMvip, pUsername, pPassword, "ListAccounts", {})

    # Find the source account if the user specified one
    source_account_id = 0
    if AccountName or AccountId:
        if AccountId:
            source_account_id = int(AccountId)
        elif AccountName:
            account_info = FindAccount(pMvip, pUsername, pPassword, AccountName=AccountName)
            source_account_id = account_info["accountID"]
        params = {}
        params["accountID"] = source_account_id
        account_volumes = CallApiMethod(pMvip, pUsername, pPassword, "ListVolumesForAccount", params)

    found_volumes = dict()
    count = 0

    # Search for specific volume id or list of ids
    if VolumeId:
        # Convert to a list if it is a scalar
        volume_id_list = []
        if isinstance(VolumeId, basestring):
            volume_id_list = VolumeId.split(",")
            volume_id_list = map(int, volume_id_list)
        else:
            try:
                volume_id_list = [VolumeId]
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
        if isinstance(VolumeName, basestring):
            volume_name_list = VolumeName.split(",")
        else:
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
                        continue
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
        if isinstance(VolumeName, basestring):
            volume_name_list = VolumeName.split(",")
        else:
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
                        continue
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
                    continue
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
                    continue
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
                    continue
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
                    continue
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
                continue
            else:
                found_volumes[vol_name] = vol_id
                count += 1
                if VolumeCount > 0 and count >= VolumeCount: break

    # Search for all volumes
    else:
        for volume in all_volumes["volumes"]:
            vol_id = int(volume["volumeID"])
            vol_name = volume["name"]
            if vol_name in found_volumes.keys():
                mylog.warning("Duplicate volume name " + vol_name)
                continue
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
    retcode = None
    stdout = ""
    stderr = ""
    while retry > 0:
        cmd = "ipmitool -Ilanplus -U" + str(IpmiUsername) + " -P" + str(IpmiPassword) + " -H" + str(IpmiIp) + " -E " + str(IpmiCommand)
        mylog.debug("Executing " + cmd)
        retcode, stdout, stderr = RunCommand(cmd)
        if retcode == 0:
            break
        retry -= 1
        time.sleep(3)
    if retcode != 0:
        raise SfError("ipmitool error: " + stdout + stderr)

def ClusterIsBinSyncing(Mvip, Username, Password):
    version = CallApiMethod(Mvip, Username, Password, "GetClusterVersionInfo", {})
    cluster_version = float(version["clusterVersion"])
    if cluster_version >= 5.0:
        # Get the bin assignments report
        result = HttpRequest("https://" + Mvip + "/reports/bins.json", Username, Password)
        bin_report = json.loads(result)

        # Make sure that all bins are active and not syncing
        for bsbin in bin_report:
            for service in bsbin["services"]:
                if service["status"] != "bsActive":
                    mylog.debug("Bin sync - one or more bins are not active")
                    return True
    else:
        # Get the bin syncing report
        result = HttpRequest("https://" + Mvip + "/reports/binsyncing", Username, Password)
        if "<table>" in result:
            mylog.debug("Bin sync - entries in bin syncing report")
            return True

    # Make sure there are no block related faults
    result = CallApiMethod(Mvip, Username, Password, "ListClusterFaults", {'faultTypes' : 'current'})
    for fault in result["faults"]:
        if fault["code"] == "blockServiceUnhealthy":
            mylog.debug("Bin sync - block related faults are present")
            return True

    return False

def ClusterIsSliceSyncing(Mvip, Username, Password):
    version = CallApiMethod(Mvip, Username, Password, "GetClusterVersionInfo", {})
    cluster_version = float(version["clusterVersion"])
    if cluster_version >= 5.0:
        # Get the slice assignments report
        result = HttpRequest("https://" + Mvip + "/reports/slices.json", Username, Password)
        slice_report = json.loads(result)

        # Make sure there are no unhealthy services
        if "service" in slice_report:
            for ss in slice_report["services"]:
                if ss["health"] != "good":
                    mylog.debug("Slice sync - one or more SS are unhealthy")
                    return True

        # Make sure there are no volumes with multiple live secondaries or dead secondaries
        if "slice" in slice_report:
            for vol in slice_report["slices"]:
                if "liveSecondaries" not in vol:
                    mylog.debug("Slice sync - one or more volumes have no live secondaries")
                    return True
                if len(vol["liveSecondaries"]) > 1:
                    mylog.debug("Slice sync - one or more volumes have multiple live secondaries")
                    return True
                if "deadSecondaries" in vol and len(vol["deadSecondaries"]) > 0:
                    mylog.debug("Slice sync - one or more volumes have dead secondaries")
                    return True
    else:
        # Get the slice syncing report
        result = HttpRequest("https://" + Mvip + "/reports/slicesyncing", Username, Password)
        if "<table>" in result:
            mylog.debug("Slice sync - entries in slice syncing report")
            return True

    # Make sure there are no slice related faults
    result = CallApiMethod(Mvip, Username, Password, "ListClusterFaults", {'faultTypes' : 'current'})
    for fault in result["faults"]:
        if fault["code"] == "sliceServiceUnhealthy" or fault["code"] == "volumesDegraded":
            mylog.debug("Slice sync - slice related faults are present")
            return True

    return False

def FindAccount(Mvip, Username, Password, AccountName=None, AccountId=None):
    if not AccountName and not AccountId:
        raise SfError("Please specify either AcountName or AccountId")


    account_list = CallApiMethod(Mvip, Username, Password, "ListAccounts", {})
    if AccountName:
        for account in account_list["accounts"]:
            if (account["username"].lower() == str(AccountName).lower()):
                return account
        raise SfError("Could not find account with name " + str(AccountName))

    else:
        try:
            AccountId = int(AccountId)
        except TypeError:
            raise SfError("Please specify an integer for AccountId")
        for account in account_list["accounts"]:
            if account["accountID"] == AccountId:
                return account
        raise SfError("Could not find account with ID " + str(AccountId))

def FindVolumeAccessGroup(Mvip, Username, Password, VagName=None, VagId=None, ApiVersion=5.0):
    if not VagName and not VagId:
        raise SfError("Please specify either VagName or VagId")

    vag_list = CallApiMethod(Mvip, Username, Password, "ListVolumeAccessGroups", {}, ApiVersion=ApiVersion)
    if VagName:
        for vag in vag_list["volumeAccessGroups"]:
            if vag["name"].lower() == VagName.lower():
                return vag
        raise SfError("Could not find group with name " + str(VagName))

    else:
        try:
            VagId = int(VagId)
        except ValueError:
            raise SfError("Please specify an integer for VagId")
        for vag in vag_list["volumeAccessGroups"]:
            if vag["volumeAccessGroupID"] == VagId:
                return vag
        raise SfError("Couldnot find group with ID " + str(VagId))

def ValidateArgs(argsToValidate, argsPassed):
    """Validate arguments

    Args:
        args_to_validate: a dictionary of argument names to validator functions.  If the validator function is None, just validate the argument is present and has a value
        args_passed: a dictionary of argument names to argument values

    Raises:
        SfArgumentError: if there is a missing/invalid argument
    """

    errors = []
    for arg_name in argsToValidate.keys():
        if arg_name not in argsPassed:
            errors.append("Missing argument '" + arg_name + "'")
        else:
            arg_value = argsPassed[arg_name]
            if arg_value == None or len(str(arg_value)) < 1:
                errors.append("Missing value for '" + arg_name + "'")
            elif argsToValidate[arg_name] and not argsToValidate[arg_name](arg_value):
                errors.append("Invalid value for '" + arg_name + "'")
    if errors:
        raise SfArgumentError("\n".join(errors))


def ThreadRunner_counter(threadList, resultList, concurrentThreadCount):
    """Run a list of threads at a specified concurrency level

    Args:
        threadList: the list of thread or process objects to be run
        resultList: the dictionary results are stored in, with key string threadname => value boolean result
        concurrentThreadCount: max number of threads to execute in parallel

    Returns:
        A tuple of True if all results evaluated true/False if any thread result failed, Successful thread counter.
    """
    running_threads = []
    for th in threadList:
        # If we are above the thread count, wait for at least one thread to finish
        while len(running_threads) >= concurrentThreadCount:
            for i in range(len(running_threads)):
                if not running_threads[i].is_alive():
                    del running_threads[i]
                    break
        th.start()
        running_threads.append(th)

    # Wait for all threads to be done
    for th in running_threads:
        th.join()

    # Check the results
    success_threads = 0
    for i, val in resultList.items():
        if val is True:
            success_threads += 1
            mylog.debug("Thread " + str(i) + " succeeded")
        else:
            mylog.debug("Thread " + str(i) + " failed")
            if isinstance(val, Exception):
                raise val

    #for res in resultList.values():
    #    if res:
    #        success_threads += 1

    if success_threads == len(threadList):
        return (True, success_threads)
    else:
        return (False, success_threads)


def ThreadRunner(threadList, resultList, concurrentThreadCount):
    """Run a list of threads at a specified concurrency level

    Args:
        threadList: the list of thread or process objects to be run
        resultList: the dictionary results are stored in, with key string threadname => value boolean result
        concurrentThreadCount: max number of threads to execute in parallel

    Returns:
        True if all results evaluated true, False if any thread result failed
    """
    result, counter = ThreadRunner_counter(threadList, resultList, concurrentThreadCount)
    return result

def CallbackWrapper(callback):
    """Create a safe callback that does not throw exceptions

    KeyboardInterrupt and SystemExit are raised but all other exceptions are suppressed
    Suppressed exceptions will print a warning and then return

    Args:
        callback: the function to call

    Returns:
        The wrapped callback function
    """

    def wrapped(*args, **kwargs):
        try:
            callback(*args, **kwargs)
        except KeyboardInterrupt:
            raise
        except SystemExit:
            raise
        except Exception as e:
            mylog.warning("Exception in callback: " + str(e))

    return wrapped

def GuessHypervisor():
    """Attempt to determine if I am a guest and which hypervisor I am in

    Returns:
        A string hypervisor name, or None if this is a physical machine
    """
    if platform.system().lower().startswith("win"):
        command = 'systeminfo | find "System Manufacturer"'
        retcode, stdout, stderr = RunCommand(command)
        if retcode != 0:
            raise SfError("Could not get system info: " + stderr)
        if "VMware" in stdout:
            return "ESX"
        elif "Xen" in stdout:
            return "Xen"
        else:
            return None

    else:
        retcode, stdout, stderr = RunCommand("virt-what")
        if retcode != 0:
            raise SfError("virt-what failed: " + stderr)
        output = stdout.strip().lower()
        # Order is important for these tests! sometimes virt-what will repond with multiple answers
        if not output:
            return None
        elif "xen" in output:
            return "Xen"
        elif "kvm" in output:
            return "KVM"
        elif "hyperv" in output:
            return "HyperV"
        elif "vmware" in output:
            return "ESX"
        else:
            return output

def CimDatetimeToTimestamp(cimDatetime):
    """Convert a CIM formatted datetime into a unix timestamp
    yyyymmddHHMMSS.mmmmmmsUUU
    20131105183538.493757-420

    Args:
        cimDatetime: a string containing a CIM datetime

    Returns:
        An integer unix timestamp
    """
    if "-" in cimDatetime:
        pieces = cimDatetime.split("-")
        time_str = pieces[0]
        offset = 0 - int(pieces[1])
    elif "+" in cimDatetime:
        pieces = cimDatetime.split("+")
        time_str = pieces[0]
        offset = int(pieces[1])
    else:
        time_str = cimDatetime
        offset = 0

    time_str = time_str.split(".")[0]

    parsed = None
    try:
        parsed = datetime.datetime.strptime(time_str, "%Y%m%d%H%M%S")
    except ValueError: pass
    if parsed:
        timestamp = calendar.timegm(parsed.timetuple())
        if timestamp <= 0:
            timestamp = calendar.timegm(parsed.utctimetuple())
        if offset > 0:
            timestamp += offset

    return timestamp

def IPToInteger(ipStr):
    """Convert a string dotted quad IP address to an integer

    Args:
        ipStr: the IP address to convert

    Returns:
        The IP address as an integer
    """
    pieces = ipStr.split(".")
    return (int(pieces[0]) << 24) + (int(pieces[1]) << 16) + (int(pieces[2]) << 8) + int(pieces[3])

def IntegerToIP(ipInt):
    """Convert an integer IP address to dotted quad notation

    Args:
        ipInt: the IP address to convert

    Returns:
        The IP address as a string in dotted quad notation
    """
    return ".".join(map(str,[(ipInt & (0xFF << (8*n))) >> 8*n for n in (3, 2, 1, 0)]))

def CalculateNetwork(ipAddress, subnetMask):
    """Calculate the network given an IP address on the network and the subnet mask of the network

    Args:
        ipAddress: an IP address on the network
        subnetMask: the mask of the network

    Returns:
        The network address in dotted quad notation
    """
    ip_int = IPToInteger(ipAddress)
    mask_int = IPToInteger(subnetMask)
    network_int = ip_int & mask_int
    return IntegerToIP(network_int)

def CalculateBroadcast(ipAddress, subnetMask):
    """Calculate the broadcast address of a network given an IP address on the network and the subnet mask of the network

    Args:
        ipAddress: an IP address on the network
        subnetMask: the mask of the network

    Returns:
        The broadcast address in dotted quad notation
    """
    ip_int = IPToInteger(ipAddress)
    mask_int = IPToInteger(subnetMask)
    bcast_int = ip_int | ~mask_int
    return IntegerToIP(bcast_int)

def CalculateNetmask(startIP, endIP):
    """Calculate the subnet mask of a network given the start and end IP

    Args:
        startIP: the first IP address in the network
        endIP: the last IP address in the network

    Returns:
        The subnet mask in dotted quad notation
    """
    start_ip_int = IPToInteger(startIP)
    end_ip_int = IPToInteger(endIP)
    mask_int = 0xFFFFFFFF ^ start_ip_int ^ end_ip_int
    return IntegerToIP(mask_int)

def ffs(num):
    """Find the lowest order bit that is set

    Args:
        num: the number to search

    Returns:
        The 0-based index of the lowest order bit that is set, or None if no bits are set
    """
    if num == 0:
        return None
    i = 0
    while (num % 2) == 0:
        i += 1
        num = num >> 1
    return i

def NetmaskToCIDR(netmask):
    """Convert dotted-quad netmask to CIDR

    Args:
        netmask: the string netmask to convert

    Returns:
        The CIDR number corresponding to the netmask
    """
    packed = socket.inet_pton(socket.AF_INET, netmask)
    int_mask = struct.unpack('!I', packed)[0]
    lsb = ffs(int_mask)
    if lsb is None:
        return 0
    cidr_mask = 32 - ffs(int_mask)
    return cidr_mask

def CIDRToNetmask(cidrMask):
    """Convert a CIDR netmask to dotted-quad string

    Args:
        cidrMask: the CIDR netmask to convert

    Returns:
        The dotted-quad string corresponding to the CIDR mask
    """
    bits = 0
    for i in xrange(32 - cidrMask, 32):
        bits |= (1 << i)
    return socket.inet_ntoa(struct.pack('>I', bits))

# Implement inet_pton and inet_ntop for Windows
# From https://gist.github.com/nnemkin/4966028 with minor modifications
if platform.system().lower().startswith("win"):
    class sockaddr(ctypes.Structure):
        _fields_ = [("sa_family", ctypes.c_short),
                    ("__pad1", ctypes.c_ushort),
                    ("ipv4_addr", ctypes.c_byte * 4),
                    ("ipv6_addr", ctypes.c_byte * 16),
                    ("__pad2", ctypes.c_ulong)]

    WSAStringToAddressA = ctypes.windll.ws2_32.WSAStringToAddressA
    WSAAddressToStringA = ctypes.windll.ws2_32.WSAAddressToStringA

    def inet_pton(address_family, ip_string):
        addr = sockaddr()
        addr.sa_family = address_family
        addr_size = ctypes.c_int(ctypes.sizeof(addr))

        if WSAStringToAddressA(ip_string, address_family, None, ctypes.byref(addr), ctypes.byref(addr_size)) != 0:
            raise socket.error(ctypes.FormatError())

        if address_family == socket.AF_INET:
            return ctypes.string_at(addr.ipv4_addr, 4)
        if address_family == socket.AF_INET6:
            return ctypes.string_at(addr.ipv6_addr, 16)

        raise socket.error('unknown address family')

    def inet_ntop(address_family, packed_ip):
        addr = sockaddr()
        addr.sa_family = address_family
        addr_size = ctypes.c_int(ctypes.sizeof(addr))
        ip_string = ctypes.create_string_buffer(128)
        ip_string_size = ctypes.c_int(ctypes.sizeof(addr))

        if address_family == socket.AF_INET:
            if len(packed_ip) != ctypes.sizeof(addr.ipv4_addr):
                raise socket.error('packed IP wrong length for inet_ntoa')
            ctypes.memmove(addr.ipv4_addr, packed_ip, 4)
        elif address_family == socket.AF_INET6:
            if len(packed_ip) != ctypes.sizeof(addr.ipv6_addr):
                raise socket.error('packed IP wrong length for inet_ntoa')
            ctypes.memmove(addr.ipv6_addr, packed_ip, 16)
        else:
            raise socket.error('unknown address family')

        if WSAAddressToStringA(ctypes.byref(addr), addr_size, None, ip_string, ctypes.byref(ip_string_size)) != 0:
            raise socket.error(ctypes.FormatError())

        return ip_string[:ip_string_size.value]

    socket.inet_pton = inet_pton
    socket.inet_ntop = inet_ntop

def HumanizeWWN(hexWWN):
    """Convert a hex WWN (0x10000090fa34ad72) to a pretty format (10:00:00:90:fa:34:ad:72)

    Args:
        hexWWN: the WWN in hex format

    Returns:
        The prettified string version of the WWN
    """
    pretty = ''
    if hexWWN.startswith('0x'):
        start_index = 2
    else:
        start_index = 0
    for i in range(start_index, 2*8+2, 2):
        pretty += ':' + hexWWN[i:i+2]
    return pretty.strip(":")
