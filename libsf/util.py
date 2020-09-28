#!/usr/bin/env python2.7
"""This module provides various utility classes and functions"""

import calendar as _calendar
import datetime as _datetime
import functools as _functools
import inspect as _inspect
import json as _json
import os as _os
import re as _re
import socket as _socket
import time as _time

from .logutil import GetLogger
from . import InvalidArgumentError
import six

# ===================================================================================================
#  Types and type checking, compatible with argparse
# ===================================================================================================

def ValidateArgs(args, validators):
    """
    Validate a list of arguments using the supplied validator functions
    
    Args:
        args:           a dictionary of arg name => arg value (dict)
        validators:     a dictionary of arg name => validator function (dict)
    
    Returns:
        The validated and type converted arguments
    """
    validated = {}
    errors = []
    for arg_name in validators.keys():
        # Make sure this argument is present
        if arg_name not in args:
            errors.append("Missing argument '{}'".format(arg_name))
        else:
            # Make sure this argument is valid using the supplied validator
            if validators[arg_name]:
                try:
                    valid = validators[arg_name](args[arg_name])
                    # Special case for BoolType, which can return False
                    if getattr(validators[arg_name], "__name__", "") != "BoolType" and valid is False:
                        errors.append("Invalid value for '{}'".format(arg_name))
                    else:
                        validated[arg_name] = valid
                except InvalidArgumentError as e:
                    errors.append("Invalid value for '{}' - {}".format(arg_name, e))
            # Make sure this argument has a value
            # boolean False or empty list is OK, but None or empty string is not
            elif args[arg_name] is None or args[arg_name] == "":
                errors.append("'{}' must have a value".format(arg_name))
    if errors:
        raise InvalidArgumentError("\n".join(errors))

    for argname, argvalue in args.items():
        if argname not in list(validated.keys()):
            validated[argname] = argvalue

    return validated

class ValidateAndDefault(object):
    """
    Decorator to validate, typecheck, and set defaults for function arguments
    """
    def __init__(self, argValidators):
        self.validators = {}
        self.defaults = {}
        for arg_name, (arg_type, arg_default) in argValidators.items():
            self.validators[arg_name] = arg_type
            self.defaults[arg_name] = arg_default

    def __call__(self, func):

        @_functools.wraps(func)
        def wrapper(*args, **kwargs):
            log = GetLogger()
            # Build a dictionary of arg name => default value from the function spec
            spec = _inspect.getargspec(func)
            arg_names = list(spec.args)
            if spec.defaults:
                arg_defaults = list(spec.defaults)
            else:
                arg_defaults = []
            while len(arg_defaults) < len(arg_names):
                arg_defaults.insert(0, None)
            default_args = dict(zip(arg_names, arg_defaults))
            # Replace any defaults with the ones supplied to this decorator
            if self.defaults:
                for arg_name, arg_default in self.defaults.items():
                    default_args[arg_name] = arg_default

            # Combine args and kwargs into a single dictionary of arg name => user supplied value
            user_args = {}
            for idx, user_val in enumerate(args):
                arg_name = arg_names[idx]
                user_args[arg_name] = user_val
            for arg_name, user_val in kwargs.items():
                user_args[arg_name] = user_val

            # Fill in and log the default values being used
            for arg_name, validator in self.validators.items():
                if arg_name not in user_args or user_args[arg_name] == None:
                    log.debug2("  Using default value {}={}".format(arg_name, default_args[arg_name]))
                    user_args[arg_name] = default_args[arg_name]

            # Run each validator against the user input
            errors = []
            valid_args = {}
            for arg_name, validator in self.validators.items():
                if arg_name not in user_args:
                    errors.append("{} must have a value".format(arg_name))
                    continue
                user_val = user_args[arg_name]

                if validator:
                    log.debug2("  Validating {}={} is a {}".format(arg_name, user_val, GetPrettiestTypeName(self.validators[arg_name])))
                    try:
                        valid_val = self.validators[arg_name](user_val)
                        valid_args[arg_name] = valid_val
                    except InvalidArgumentError as ex:
                        errors.append("invalid value for {} - {}".format(arg_name, ex))
                elif user_val is None or user_val == "":
                    errors.append("{} must have a value".format(arg_name))
                else:
                    log.debug2("  Skipping validation for {}".format(arg_name))
                    valid_args[arg_name] = user_val

            # Look for any "extra" args that were passed in
            for arg_name in user_args.keys():
                if arg_name not in list(self.validators.keys()):
                    errors.append("Unknown argument {}".format(arg_name))

            if errors:
                raise InvalidArgumentError("\n".join(errors))

            return func(**valid_args)

        setattr(wrapper, "__innerfunc__", func)
        return wrapper

def NameOrID(objName, objID, typeName):
    """Validate that either a name or ID was passed in"""
    if not objName:
        try:
            objID = SolidFireIDType(objID)
        except InvalidArgumentError:
            raise InvalidArgumentError("Please specify either a name or ID for the {}".format(typeName))
    return objName, objID

def IsSet(value, name=None):
    """
    Validate that the value is not None or an empty string
    Boolean false or an empty list are OK
    """
    if value is None or value == "":
        if name:
            raise InvalidArgumentError("{} must have a value".format(name))
        else:
            raise InvalidArgumentError("Argument must have a value")
    return value

def StrType(string, allowEmpty=False, name=None):
    """Type for validating strings"""
    if string:
        string = str(string)

    if string is None or (string == "" and not allowEmpty):
        if name:
            raise InvalidArgumentError("{} must have a value".format(name))
        else:
            raise InvalidArgumentError("Argument must have a value")

    return string

class SelectionType(object):
    """Type for making a choice from a list of options"""

    def __init__(self, choices, itemType=StrType):
        if not callable(itemType):
            raise ValueError("type must be callable")
        self.choices = choices
        self.itemType = itemType

    def __call__(self, string):
        # Verify that the selection is one of the choices
        try:
            sel = self.itemType(string)
        except (TypeError, ValueError):
            raise InvalidArgumentError("'{}' is not a valid {}".format(string, GetPrettiestTypeName(self.itemType)))

        if sel not in self.choices:
            raise InvalidArgumentError("'{}' is not a valid choice".format(string))

        return sel

    def __repr__(self):
        return "SelectionType({}) [{}]".format(GetPrettiestTypeName(self.itemType), ",".join([str(c) for c in self.choices]))

class ItemList(object):
    """Type for making a list of things"""

    def __init__(self, itemType=StrType, allowEmpty=False):
        if not callable(itemType) and itemType is not None:
            raise ValueError("type must be callable or None")
        self.itemType = itemType
        self.allowEmpty = allowEmpty

    def __call__(self, string):
        # Split into individual items
        if string is None:
            items = []
        elif isinstance(string, six.string_types):
            items = [i for i in _re.split(r"[,\s]+", string) if i]
        else:
            try:
                items = list(string)
            except TypeError:
                items = []
                items.append(string)

        # Validate each item is the correct type
        try:
            if self.itemType is not None:
                items = [self.itemType(i) for i in items]
        except (TypeError, ValueError):
            raise InvalidArgumentError("Invalid {} value".format(GetPrettiestTypeName(self.itemType)))

        # Validate the list is not empty
        if not self.allowEmpty and not items:
            raise InvalidArgumentError("list cannot be empty")

        return items

    def __repr__(self):
        return "list({})".format(GetPrettiestTypeName(self.itemType))

class OptionalValueType(object):
    """Type for validating an optional"""

    def __init__(self, itemType=StrType):
        if not callable(itemType) and itemType is not None:
            raise ValueError("type must be callable or None")
        self.itemType = itemType

    def __call__(self, string):
        if string is None:
            return None

        try:
            if self.itemType is None:
                item = string
            else:
                item = self.itemType(string)
        except (TypeError, ValueError):
            raise InvalidArgumentError("{} is not a valid {}".format(string, GetPrettiestTypeName(self.itemType)))
        return item

    def __repr__(self):
        return "OptionalValueType({})".format(GetPrettiestTypeName(self.itemType))

def AtLeastOneOf(**kwargs):
    """Validate that one or more of the list of items has a value"""
    if not any(kwargs.values()):
        raise InvalidArgumentError("At least one of [{}] must have a value".format(",".join(list(kwargs.keys()))))

def BoolType(string, name=None):
    """Type for validating boolean"""
    if isinstance(string, bool):
        return string

    string = str(string).lower()
    if string in ["f", "false"]:
        return False
    elif string in ["t", "true"]:
        return True

    if name:
        raise InvalidArgumentError("Invalid boolean value for {}".format(name))
    else:
        raise InvalidArgumentError("Invalid boolean value")


def IPv4AddressType(addressString, allowHostname=True):
    """Type for validating IP v4 addresses"""

    if allowHostname:
        errormsg = "{} is not a resolvable hostname or valid IP address".format(addressString)
    else:
        errormsg = "{} is not a valid IP address".format(addressString)

    if not addressString:
        raise InvalidArgumentError("missing value")

    # Check for resolvable hostname
    if any (c.isalpha() for c in addressString):
        if allowHostname:
            return ResolvableHostname(addressString)
        else:
            raise InvalidArgumentError("{} is not a valid IP address".format(addressString))

    try:
        _socket.inet_pton(_socket.AF_INET, addressString)
        return addressString
    except AttributeError: # inet_pton not available
        try:
            _socket.inet_aton(addressString)
            return addressString
        except _socket.error:
            raise InvalidArgumentError(errormsg)
    except _socket.error: # not a valid address
        raise InvalidArgumentError(errormsg)

    pieces = addressString.split(".")
    if len(pieces) != 4:
        raise InvalidArgumentError(errormsg)

    try:
        pieces = [int(i) for i in pieces]
    except ValueError:
        raise InvalidArgumentError(errormsg)

    if not all([i >= 0 and i <= 255 for i in pieces]):
        raise InvalidArgumentError(errormsg)

    return addressString

def IPv4AddressOnlyType(addressString):
    """Type for validating IPv4 addresses"""
    return IPv4AddressType(addressString, allowHostname=False)

def ResolvableHostname(hostnameString):
    """Type for validating a string is a resolvable hostname"""

    hostnameString = StrType(hostnameString)

    if not hostnameString:
        raise InvalidArgumentError("missing value")

    try:
        _socket.gethostbyname(hostnameString)
    except _socket.gaierror: #Unable to resolve host name
        raise InvalidArgumentError("{} is not a resolvable hostname".format(hostnameString))

    return hostnameString

def IPv4SubnetType(subnetString):
    """Type for validating subnets, either CIDR or network/netmask"""
    if not subnetString:
        raise InvalidArgumentError("missing value")

    if "/" not in subnetString:
        raise InvalidArgumentError("missing CIDR bits or netmask")

    network, mask = subnetString.split("/")

    # Validate the network is a valid IP address
    IPv4AddressType(network, allowHostname=False)

    # Validate the mask is either a valid IP, or an integer between 0 and 32
    try:
        IPv4AddressType(mask, allowHostname=False)
        return subnetString
    except InvalidArgumentError:
        pass

    try:
        IntegerRangeType(0, 32)(mask)
        return subnetString
    except InvalidArgumentError:
        pass

    raise InvalidArgumentError("invalid CIDR bits or netmask")

def SolidFireIDType(string):
    """Type for validating SolidFire IDs"""

    errormsg = "{} is not a positive, non-zero integer".format(string)

    try:
        number = int(string)
    except (TypeError, ValueError):
        raise InvalidArgumentError(errormsg)

    if number <= 0:
        raise InvalidArgumentError(errormsg)
    return number

class SolidFireVolumeSizeType(object):
    
    def __init__(self,gib=False):
        self.gib = gib

    def __call__(self, string):
        return IntegerRangeType(1, 7450 if self.gib else 8000)(string)

def SolidFireMinIOPSType(string):
    return IntegerRangeType(50, 15000)(string)

def SolidFireMaxIOPSType(string):
    return IntegerRangeType(100, 100000)(string)

def SolidFireBurstIOPSType(string):
    return IntegerRangeType(100, 100000)(string)

class CountType(object):
    """Type for validating a count of something"""

    def __init__(self, allowZero=False):
        if allowZero:
            self.minval = 0
        else:
            self.minval = 1

    def __call__(self, string):
        return IntegerRangeType(self.minval)(string)

class IntegerRangeType(object):
    """Type for validating an integer within a range of values, inclusive"""

    def __init__(self, minValue=None, maxValue=None):
        self.minValue = None
        self.maxValue = None

        if minValue is not None:
            self.minValue = int(minValue)
        if maxValue is not None:
            self.maxValue = int(maxValue)

    def __call__(self, string):

        try:
            number = int(string)
        except (TypeError, ValueError):
            raise InvalidArgumentError("{} is not a valid integer".format(string))
        
        if self.minValue is not None and number < self.minValue:
            raise InvalidArgumentError("{} must be >= {}".format(number, self.minValue))

        if self.maxValue is not None and number > self.maxValue:
            raise InvalidArgumentError("{} must be <= {}".format(number, self.maxValue))

        return number

def PositiveIntegerType(string):
    """Type for validating integers"""

    errormsg = "{} is not a positive integer".format(string)

    try:
        number = int(string)
    except (TypeError, ValueError):
        raise InvalidArgumentError(errormsg)

    if number < 0:
        raise InvalidArgumentError(errormsg)
    return number

def PositiveNonZeroIntegerType(string):
    """Type for validating integers"""

    errormsg = "{} is not a positive non-zero integer".format(string)

    try:
        number = int(string)
    except (TypeError, ValueError):
        raise InvalidArgumentError(errormsg)

    if number <= 0:
        raise InvalidArgumentError(errormsg)
    return number

def VLANTagType(string):
    """Type for validating VLAN tags"""

    errormsg = "{} is not a valid VLAN tag".format(string)

    try:
        tag = int(string)
    except (TypeError, ValueError):
        raise InvalidArgumentError(errormsg)
    if tag < 1 or tag > 4095:
        raise InvalidArgumentError(errormsg)
    return tag

def MACAddressType(string):
    """Type for validating MAC address"""

    errormsg = "{} is not a valid MAC address".format(string)

    if not _re.match("[0-9a-f]{2}([-:])[0-9a-f]{2}(\\1[0-9a-f]{2}){4}$", string):
        raise InvalidArgumentError(errormsg)
    return string.lower()

def RegexType(string):
    """Type for validating regexes"""

    try:
        _re.compile(string)
    except _re.error:
        raise InvalidArgumentError("Invalid regex")
    return string

def GetPrettiestTypeName(typeToName):
    """Get the best human representation of a type"""
    if typeToName is None:
        return "Any"
    typename = repr(typeToName)
    # Hacky
    if typename.startswith("<"):
        typename = getattr(typeToName, "__name__", str(typeToName))
    return typename

# ===================================================================================================
#  Time manipulation
# ===================================================================================================

class LocalTimezone(_datetime.tzinfo):
    """Class representing the time zone of the machine we are currently running on"""

    def __init__(self):
        super(LocalTimezone, self).__init__()
        self.STDOFFSET = _datetime.timedelta(seconds = -_time.timezone)
        if _time.daylight:
            self.DSTOFFSET = _datetime.timedelta(seconds = -_time.altzone)
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
            return _datetime.timedelta(0)

    def tzname(self, dt):
        return _time.tzname[self._isdst(dt)]

    def _isdst(self, dt):
        """check if this _timezone is in daylight savings _time"""
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = _time.mktime(tt)
        tt = _time.localtime(stamp)
        return tt.tm_isdst > 0

#pylint: disable=unused-argument
class UTCTimezone(_datetime.tzinfo):
    """Class representing UTC time"""

    def utcoffset(self, dt):
        return _datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return _datetime.timedelta(0)
#pylint: enable=unused-argument

def ParseDateTime(timeString):
    """
    Parse a string into a _datetime
    
    Args:
        timeString: the string containing a parsable date/time
    
    Returns:
        A datetime object corresponding to the date/time in the string
    """

    known_formats = [
        "%Y-%m-%d %H:%M:%S.%f",     # old sf format
        "%Y-%m-%dT%H:%M:%S.%fZ",    # ISO format with UTC timezone
        "%Y-%m-%dT%H:%M:%SZ",       # ISO format without ffractional seconds and UTC timezone
        "%Y%m%dT%H:%M:%SZ",
        "%b %d %H:%M:%S"            # syslog/date format
    ]
    parsed = None
    for fmt in known_formats:
        try:
            parsed = _datetime.datetime.strptime(timeString, fmt)
            break
        except ValueError: pass
    
    if parsed.year == 1900:
        parsed = parsed.replace(year=_datetime.datetime.now().year)

    return parsed

def ParseTimestamp(timeString):
    """
    Parse a string into a unix timestamp
    
    Args:
        timeString: the string containing a parsable date/time
    
    Returns:
        An integer timestamp corresponding to the date/time in the string
    """
    date_obj = ParseDateTime(timeString)
    if (date_obj != None):
        timestamp = _calendar.timegm(date_obj.timetuple())
        if timestamp <= 0:
            timestamp = _calendar.timegm(date_obj.utctimetuple())
        return timestamp
    else:
        return 0

def ParseTimestampHiRes(timeString):
    """
    Parse a string into a unix timestamp with floating point microseconds
    
    Args:
        timeString: the string containing a parsable date/time
    
    Returns:
        An floating point timestamp corresponding to the date/time in the string
    """
    date_obj = ParseDateTime(timeString)
    if (date_obj != None):
        return (_calendar.timegm(date_obj.timetuple()) + date_obj.microsecond)
    else:
        return 0

def TimestampToStr(timestamp, formatString = "%Y-%m-%d %H:%M:%S", timeZone = LocalTimezone()):
    """
    Convert a _timestamp to a human readable string
    
    Args:
        _timeStamp:      the _timestamp to convert
        formatString:   the format to convert to
        _timeZone:       the _time zone to convert to
    
    Returns:
        A string containing the date/_time in the requested format and _time zone
    """
    display_time = _datetime.datetime.fromtimestamp(timestamp, timeZone)
    return display_time.strftime(formatString)

def SecondsToElapsedStr(seconds):
    """
    Convert an integer number of seconds into elapsed time format (D-HH:MM:SS)

    Args:
        seconds:    the total number of seconds (int)

    Returns:
        A formatted elapsed time (str)
    """
    if isinstance(seconds, six.string_types):
        return seconds

    delta = _datetime.timedelta(seconds=seconds)
    return TimeDeltaToStr(delta)

def TimeDeltaToStr(timeDelta):
    """
    Convert a timedelta object to an elapsed time format (D-HH:MM:SS)

    Args:
        timeDelta:  a timedelta object containing the total time (datetime.timedelta)

    Returns:
         Formatted elapsed time (str)
    """
    days = timeDelta.days
    hours = 0
    minutes = 0
    seconds = timeDelta.seconds
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

# ===================================================================================================
#  Pretty formatting
# ===================================================================================================

def HumanizeBytes(totalBytes, precision=1, suffix=None):
    """
    Convert a number of bytes into the appropriate pretty kiB, MiB, etc.

    Args:
        totalBytes: the number to convert
        precision:  how many decimal numbers of precision to preserve
        suffix:     use this suffix (kiB, MiB, etc.) instead of automatically determining it

    Returns:
        The prettified string version of the input
    """
    if (totalBytes == None):
        return "0 B"

    converted = float(totalBytes)
    suffix_index = 0
    suffix_list = ['B', 'kiB', 'MiB', 'GiB', 'TiB']

    while (abs(converted) >= 1000):
        converted /= 1024.0
        suffix_index += 1
        if suffix_list[suffix_index] == suffix:
            break

    return "{0:.{1}f} {2}".format(converted, precision, suffix_list[suffix_index])

def HumanizeDecimal(number, precision=1, suffix=None):
    """
    Convert a number into the appropriate pretty k, M, G, etc.

    Args:
        totalBytes: the number to convert
        precision:  how many decimal numbers of precision to preserve
        suffix:     use this suffix (k, M, etc.) instead of automatically determining it

    Returns:
        The prettified string version of the input
    """
    if (number == None):
        return "0"

    if (abs(number) < 1000):
        return str(number)

    converted = float(number)
    suffix_index = 0
    suffix_list = [' ', 'k', 'M', 'G', 'T']

    while (abs(converted) >= 1000):
        converted /= 1000.0
        suffix_index += 1
        if suffix_list[suffix_index] == suffix: break

    return "{:.{}f {}}".format(converted, precision, suffix_list[suffix_index])

def HumanizeWWN(hexWWN):
    """Convert a hex WWN (0x10000090fa34ad72) to a pretty format (10:00:00:90:fa:34:ad:72)

    Args:
        hexWWN: the WWN in hex format

    Returns:
        The prettified string version of the input
    """
    pretty = ''
    if hexWWN.startswith('0x'):
        start_index = 2
    else:
        start_index = 0
    for i in range(start_index, 2*8+2, 2):
        pretty += ':' + hexWWN[i:i+2]
    return pretty.strip(":")

def PrettyJSON(obj):
    """
    Get a pretty printed representation of an object

    Args:
        obj:    a dictionary to pretty-fy (dict)

    Returns:
        A string of pretty JSON (str)
    """
    return _json.dumps(obj, indent=2, sort_keys=True)

# ===================================================================================================
#  Misc
# ===================================================================================================

class SolidFireVersion(object):
    """Easily compare SolidFire version strings"""

    def __init__(self, versionString):
        self.rawVersion = versionString
        pieces = versionString.split(".")
        if len(pieces) == 4:
            self.major = int(pieces[0])
            self.minor = int(pieces[1])
            self.patch = int(pieces[2])
            self.build = int(pieces[3])
        elif len(pieces) == 2:
            self.major = int(pieces[0])
            self.minor = 0
            self.patch = 0
            self.build = int(pieces[1])
        self.apiVersion = float(self.major) + float(self.minor)/10

    @staticmethod
    def FromParts(major, minor, patch=None, build=None):
        return SolidFireVersion("{}.{}.{}.{}".format(major, minor, patch, build))

    def parts(self):
        return self.major, self.minor, self.patch, self.build

    def __str__(self):
        return self.rawVersion

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.major == other.major and \
               self.minor == other.minor and \
               self.patch == other.patch and \
               self.build == other.build

    def __gt__(self, other):
        return self.major > other.major or \
               (self.major == other.major and self.minor > other.minor) or \
               (self.major == other.major and self.minor == other.minor and self.patch > other.patch) or \
               (self.major == other.major and self.minor == other.minor and self.patch == other.patch and self.build > other.build)

    def __ne__(self, other):
        return not self == other

    def __ge__(self, other):
        return self == other or self > other

    def __lt__(self, other):
        return self != other and not self > other

    def __le__(self, other):
        return self == other or self < other

def GetFilename(baseName):
    """
    Get a unique filename that does not already exist. The name is generated by appending a number to the end of baseName

    Args:
        baseName:   the name to start from
    """
    filename = baseName
    idx = 0
    while _os.path.exists(filename):
        idx += 1
        filename = "{}.{}".format(baseName, idx)
    return filename

def EnsureKeys(dictionary, keyList, defaultValue=None):
    """
    Ensure that the given dictionary contains the given keys.
    If the dict does not have the key, create it with the given default value

    Args:
        dictionary:     the dict to operate on (dict)
        keyList:        the keys to ensure (list of str)
        defaultValue    the default value to set if the key does not exist
    """
    for keyname in keyList:
        if keyname not in dictionary:
            dictionary[keyname] = defaultValue
