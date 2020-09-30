#!/usr/bin/env python
"""This module provides classes for connecting to SolidFire nodes/clusters via SSH and HTTP endpoints"""

#pylint: disable=unidiomatic-typecheck,protected-access,global-statement

from __future__ import print_function, absolute_import

# Suppress warnings in python2 to hide deprecation messages in imports
import sys
if sys.version[0] == '2':
    import warnings
    warnings.filterwarnings('ignore')
import base64
import six.moves.BaseHTTPServer
import copy
import six.moves.http_client
import inspect
from io import open
import json
import os
import paramiko
import random
import socket
import ssl
import time
import six.moves.urllib.parse
import six.moves.urllib.error
# For some reason pylint 1.9 in python2.7 chokes on this import line
import six.moves.urllib.request #pylint: disable=import-error

from .logutil import GetLogger
from . import sfdefaults

class SolidFireError(Exception):
    """Base class for SolidFire exceptions"""

    def __init__(self, message, originalTraceback=None, innerException=None):
        super(SolidFireError, self).__init__(message)
        self.originalTraceback = originalTraceback
        self.innerException = innerException

    def IsRetryable(self):
        return False

    def ToDict(self):
        """Convert this exception to a dictionary"""
        return {k:copy.deepcopy(v) for k,v in vars(self).items() if not k.startswith('_')}

    def ToJSON(self):
        """Convert this exception to a JSON string"""
        return json.dumps(self.ToDict())

class InvalidArgumentError(SolidFireError):
    """Exception raised when invalid arguments are passed to a function or invalid type conversion is attempted"""

class UnknownObjectError(SolidFireError):
    """Exception raised when the specified object being searched for/operated on cannot be found"""

class UnknownNodeError(UnknownObjectError):
    """Exception raised when making a per-node API call to a non-existent nodeID"""

class SFTimeoutError(SolidFireError):
    """Exception raised when a timeout expires"""

class SolidFireAPIError(SolidFireError):
    """Exception raised when an error is returned from a SolidFire API call"""

    def __init__(self, method, params, ip, endpoint, name, code, message):
        """
        Initialize this exception with API call context

        Args:
            method:     the SolidFire API method name (e.g. GetClusterInfo)
            params:     the SolidFire API method params (e.g. {"arg1" : "value"} )
            ip:         the IP address of the SolidFire endpoint (cluster MVIP or node MIP)
            endpoint:   the full SolidFire endpoint URL (e.g. https://ip:443/json-rpc/version)
            name:       the SolidFire exception name
            code:       the SolidFire error code
            message:    the SolidFire error message
        """
        super(SolidFireAPIError, self).__init__(message)
        self.args = (method, params, ip, endpoint, name, code, message) # important to set args so this object is picklable
        self.method = method
        self.params = params
        self.ip = ip
        self.endpoint = endpoint
        self.name = name
        self.message = message.strip()
        self.code = code

    def __str__(self):
        return "{} server=[{}] method=[{}], params=[{}] - error name=[{}], message=[{}], code=[{}]".format(self.__class__.__name__, self.ip, self.method, self.params, self.name, self.message, self.code)

    def IsRetryable(self):
        return self.name in [
            'xDBConnectionLoss',
            'xDBOperationTimeout',
            'xDBSessionExpired',
            'xDBSessionMoved',
            'xDBNoServerResponse',
            'xDBClosing',
            'xDBInvalidState'
            ]

    def IsUnknownAPIError(self):
        return self.name in [
            'xUnknownAPIMethod',
            'xUnknownAPIVersion',
            'xUnknownRPCMethod'
        ]

class SFConnectionError(SolidFireError):
    """Exception raised when there is a network/connection issue communicating with the SolidFire endpoint"""

    def __init__(self, ip, endpoint, innerException, method=None, params=None, message=None, code=None):
        """
        Initialize this exception with another exception and context

        Arguments:
            ip:                 the IP address of the SolidFire endpoint (cluster MVIP or node MIP)
            endpoint:           the full SolidFire endpoint URL (e.g. https://ip:443/json-rpc/version)
            innerException:     the original exception that was thrown
            method:             the SolidFire API method name (e.g. GetClusterInfo)
            params:             the SolidFire API method params (e.g. {"arg1" : "value"} )
            message:            the exception message. This is used to override the default behavior of filling in the
                                message automatically based on innerException
            code:               the error code.  This is used to override the default behavior of filling in the
                                code automatically based on innerException
        """

        super(SFConnectionError, self).__init__(message, innerException=innerException)
        self.args = (ip, endpoint, innerException, method, params, message, code)
        self.method = method
        self.params = params
        self.ip = ip
        self.endpoint = endpoint
        self.message = message
        self.code = code
        self.retryable = False

        # If the caller did not specify a message, parse out the message, code, and retry-ability from the exception
        if not self.message and self.innerException:

            # If this exception is actually a wrapper around another exception, get the inner
            # Mostly URLError wrapping an OSError, socket.error or ssl.SSLError
            innerReason = getattr(self.innerException, "reason", None)
            if innerReason and isinstance(innerReason, Exception):
                self.innerException = innerReason

            if type(self.innerException) == six.moves.urllib.error.HTTPError:
                if self.innerException.code in six.moves.BaseHTTPServer.BaseHTTPRequestHandler.responses:
                    self.message = 'HTTP Error {}: {}'.format(self.innerException.code, six.moves.BaseHTTPServer.BaseHTTPRequestHandler.responses[self.innerException.code])
                    self.code = self.innerException.code
                else:
                    self.message = 'HTTP Error {}: {}'.format(self.innerException.code, self.innerException.reason)
                    self.code = self.innerException.code
                # 401 - unauthorized
                # 404 - not found
                if self.code not in [401, 404]:
                    self.retryable = True
            elif type(self.innerException) == six.moves.urllib.error.URLError:
                self.message = '{}'.format(self.innerException.reason)
                self.retryable = True
            elif type(self.innerException) == socket.timeout:
                self.message = 'Socket error 110: connection timed out'
                self.code = 110
                self.retryable = True
            elif type(self.innerException) in [socket.herror, socket.gaierror]:
                self.message = 'Socket error {}: {}'.format(self.innerException.args[0], self.innerException.args[1])
                self.code = self.innerException.args[0]
                #  54  - connection reset by peer
                #  61  - connection refused (transient on restarts)
                #  104 - connection reset by peer
                #  110 - connection timed out
                #  111 - connection refused (transient on restarts)
                #  113 - no route to host (transient when node is rebooted)
                if self.code in (54, 60, 61, 104, 110, 111, 113):
                    self.retryable = True
            elif type(self.innerException) == OSError:
                self.message = 'OSError {}: {}'.format(self.innerException.errno, self.innerException.strerror)
                self.code = self.innerException.errno
            elif type(self.innerException) == IOError:
                self.message = 'IOError {}: {}'.format(self.innerException.errno, self.innerException.strerror)
                self.code = self.innerException.errno
            elif type(self.innerException) == six.moves.http_client.BadStatusLine:
                self.message = 'Bad HTTP status'
                self.retryable = True
            elif type(self.innerException) == ValueError:
                self.message = 'Received invalid JSON'
                self.retryable = True
            elif isinstance(self.innerException, ssl.SSLError):
                # https://docs.python.org/2.7/library/ssl.html#functions-constants-and-exceptions
                self.message = self.innerException.message
                self.retryable = True
                if isinstance(self.innerException, ssl.CertificateError):
                    self.retryable = False
            else:
                import pprint
                print("Unknown inner exception - {}".format(pprint.pformat(self.innerException)))
                self.message = str(self.innerException)
                self.retryable = False

    def __str__(self):

        # API calls:
        # ExceptionName server=[{}] method=[{}], params=[{}] - error message=[{}], code=[{}]

        # HTTP downloads:
        # ExceptionName endpoint=[{}] - error name=[{}], message=[{}], code=[{}]

        if self.method and self.params:
            output = '{} server=[{}] method=[{}] params=[{}] - error message=[{}]'.format(self.__class__.__name__, self.ip, self.method, self.params, self.message)
        else:
            output = '{} endpoint=[{}] - error message=[{}]'.format(self.__class__.__name__, self.endpoint, self.message)
        if self.code != None:
            output += ', code=[{}]'.format(self.code)
        return output

    def IsRetryable(self):
        return self.retryable

class UnauthorizedError(SolidFireError):
    """Exception raised when an unauthorized response is returned from an SSH or HTTP SolidFire endpoint"""

    @classmethod
    def APIContext(cls, method, params, ip, endpoint):
        """
        Create this exception with API call context

        Arg:
            method:     the SolidFire API method name (e.g. GetClusterInfo)
            params:     the SolidFire API method params (e.g. {"arg1" : "value"} )
            ip:         the IP address of the SolidFire endpoint (cluster MVIP or node MIP)
            endpoint:   the full SolidFire endpoint URL (e.g. https://ip:443/json-rpc/version)
        """
        ex = cls("UnauthorizedError server=[{}] method=[{}], params=[{}] - error name=[{}], message=[{}], code=[{}]".format(ip, method, params, "xUnauthorized", "invalid credentials", 401))
        #pylint: disable=attribute-defined-outside-init
        ex.method = method
        ex.params = params
        ex.ip = ip
        ex.endpoint = endpoint
        ex.name = "xUnauthorized"
        ex.code = 401
        #pylint: enable=attribute-defined-outside-init
        return ex

    @classmethod
    def IPContext(cls, ip):
        """
        Create this exception with an IP endpoint

        Args:
            ip:     the IP address/endpoint
        """
        ex = cls("Invalid credentials for {}".format(ip))
        #pylint: disable=attribute-defined-outside-init
        ex.method = None
        ex.params = None
        ex.ip = ip
        ex.endpoint = ip
        ex.name = "xUnauthorized"
        ex.code = 401
        #pylint: enable=attribute-defined-outside-init
        return ex

    def IsRetryable(self):
        return False

class LocalEnvironmentError(SolidFireError):
    """Exception raised when something goes wrong on the local system, outside of python.
    This is basically a wrapper for python's EnvironmentError that is rooted in the SF exception hierarchy"""

    def __init__(self, innerException):
        """
        Initialize this exception with an existing exception

        Arguments:
            innerException:     the exception to wrap. It must be an exception from the EnvironmentError hierarchy (IOError, OSError)
        """

        # Make sure the input at least looks like an EnvironmentError
        assert(hasattr(innerException, 'errno'))
        assert(hasattr(innerException, 'strerror'))

        self.args = (innerException)

        if innerException.strerror:
            self.message = innerException.strerror.strip()
        else:
            self.message = str(innerException).strip()
        super(LocalEnvironmentError, self).__init__(self.message)
        self.innerException = innerException
        self.errno = innerException.errno

    def __str__(self):
        return self.message

    def IsRetryable(self):
        return False

class ClientError(SolidFireError):
    """Base for all client exceptions"""

class ClientCommandError(ClientError):
    """Exception raised when command fails on a client"""

class ClientAuthorizationError(ClientError):
    """Exception raised when an unauthorized response is returned from a client"""

class ClientRefusedError(ClientError):
    """Exception raised when a connection is refused to a client"""

class ClientConnectionError(ClientError):
    """Exception raised when there is a problem connecting to a client"""


class HTTPDownloader(object):
    """
    Download content from a URL
    """
    def __init__(self, server, port=443, username=None, password=None):
        """
        Args:
            server:             the IP address or resolvable hostname of the server to download from
            port:               the port to use
            username:           the name of an authorized user
            password:           the password of the user
        """
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.log = GetLogger()

    def Download(self, remotePath, useAuth=True, useSSL=True, timeout=300):
        """
        Download a URL (GET) and return the content. For large binary files, see StreamingDownload

        Args:
            remotePath:     the path component of the URL
            useAuth:        use Basic Auth when connecting
            useSSL:         Use SSL when connecting
            timeout:        how long to stay connected before abandoning the transfer

            The download URL will be constructed like http[s]://self.server:port/remotePath

        Returns:
            The content retrieved from the URL
        """

        context = None
        if useSSL:
            endpoint = six.moves.urllib.parse.urljoin('https://{}:{}/'.format(self.server, self.port), remotePath)

            try:
                # pylint: disable=no-member
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                # pylint: enable=no-member
            except AttributeError:
                pass

        else:
            endpoint = six.moves.urllib.parse.urljoin('http://{}:{}/'.format(self.server, self.port), remotePath)

        request = six.moves.urllib.request.Request(endpoint)
        if useAuth and self.username:
            request.add_header('Authorization', b"Basic " + base64.b64encode('{}:{}'.format(self.username, self.password).encode()).strip())

        self.log.debug2('Downloading {}'.format(endpoint))
        try:
            if context:
                # pylint: disable=unexpected-keyword-arg
                response = six.moves.urllib.request.urlopen(request, timeout=timeout, context=context)
                # pylint: enable=unexpected-keyword-arg
            else:
                response = six.moves.urllib.request.urlopen(request, timeout=timeout)
        except (socket.timeout, socket.herror, socket.gaierror) as ex:
            raise SFConnectionError(self.server, endpoint, ex)
        except six.moves.urllib.error.HTTPError as ex:
            if ex.code == 401:
                raise UnauthorizedError.IPContext(endpoint)
            else:
                raise SFConnectionError(self.server, endpoint, ex)
        except six.moves.urllib.error.URLError as ex:
            if type(ex.reason) in [socket.timeout, socket.herror, socket.gaierror]:
                raise SFConnectionError(self.server, endpoint, ex.reason)
            if type(ex.reason) == OSError:
                raise SFConnectionError(self.server, endpoint, ex.reason)
            raise SFConnectionError(self.server, endpoint, ex)
        except six.moves.http_client.BadStatusLine as ex:
            raise SFConnectionError(self.server, endpoint, ex)

        dl = response.read()
        response.close()
        return dl

    def StreamingDownload(self, remotePath, localFile, useAuth=True, useSSL=True, timeout=300):
        """
        Download a URL (GET) to a file.  Suitable for large/binary files

        Args:
            remotePath:     the path component of the URL
            localPath:      fully qualified path to the local file to save the content in. The directory
                            component of the path must already exist
            useAuth:        use Basic Auth when connecting
            useSSL:         Use SSL when connecting
            timeout:        how long to stay connected before abandoning the transfer

            The download URL will be constructed like https://self.server:port/remotePath
        """

        context = None
        if useSSL:
            endpoint = 'https://{}:{}/{}'.format(self.server, self.port, remotePath)

            try:
                # pylint: disable=no-member
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                # pylint: enable=no-member
            except AttributeError:
                pass

        else:
            endpoint = 'http://{}:{}/{}'.format(self.server, self.port, remotePath)

        request = six.moves.urllib.request.Request(endpoint)
        if useAuth and self.username:
            request.add_header('Authorization', b"Basic " + base64.b64encode('{}:{}'.format(self.username, self.password).encode()).strip())

        self.log.debug('Downloading {}'.format(endpoint))
        try:
            if context:
                # pylint: disable=unexpected-keyword-arg
                response = six.moves.urllib.request.urlopen(request, timeout=timeout, context=context)
                # pylint: enable=unexpected-keyword-arg
            else:
                response = six.moves.urllib.request.urlopen(request, timeout=timeout)
        except (socket.timeout, socket.herror, socket.gaierror) as ex:
            raise SFConnectionError(self.server, endpoint, ex)
        except six.moves.urllib.error.HTTPError as ex:
            if ex.code == 401:
                raise UnauthorizedError.IPContext(endpoint)
            else:
                raise SFConnectionError(self.server, endpoint, ex)
        except six.moves.urllib.error.URLError as ex:
            if type(ex.reason) in [socket.timeout, socket.herror, socket.gaierror]:
                raise SFConnectionError(self.server, endpoint, ex.reason)
            if type(ex.reason) == OSError:
                raise SFConnectionError(self.server, endpoint, ex.reason)
            raise SFConnectionError(self.server, endpoint, ex)
        except six.moves.http_client.BadStatusLine as ex:
            raise SFConnectionError(self.server, endpoint, ex)

        with open(localFile, 'w') as handle:
            while True:
                try:
                    chunk = response.read(16 * 1024)
                except (socket.timeout, socket.herror, socket.gaierror) as ex:
                    raise SFConnectionError(self.server, endpoint, ex)

                if not chunk:
                    break
                try:
                    handle.write(chunk)
                except IOError as ex:
                    raise LocalEnvironmentError(ex)

    @staticmethod
    def DownloadURL(url, timeout=300):
        """Convenience function for downloading a single URL"""
        pieces = six.moves.urllib.parse.urlparse(url)
        downloader = HTTPDownloader(pieces.netloc,
                                    443 if pieces.scheme == "https" else 80,
                                    pieces.username,
                                    pieces.password)
        return downloader.Download(pieces.path,
                                   useAuth=pieces.username != None,
                                   useSSL=pieces.scheme == "https",
                                   timeout=timeout)

class SolidFireAPI(object):
    """
    Base class for making SolidFire API calls - do not instantiate directly
    """

    #pylint: disable=unused-argument
    def __init__(self,
                 server,
                 username,
                 password,
                 port=443,
                 **kwargs):
        """
        Arguments:
            server:             the IP address or resolvable hostname of the node MIP or cluster MVIP
            username:           the name of an admin user
            password:           the password of the admin user
            logger:             a logging object to use. If None, no logging will be done
            maxRetryCount:      max number of times to retry an API call
            retrySleep:         how long to wait between each retry
            errorLogThreshold:  do not log any errors until at least this many have occurred
            errorLogRepeat:     after hitting errorLogThreshold, log every this many errors
        """

        self._reqid = random.randint(1, 2**24)
        self.server = server
        self.username = username
        self.password = password
        self.port = port
        self.log = kwargs.pop('logger', None)
        self.maxRetryCount = kwargs.pop('maxRetryCount', 16)
        self.retrySleep = kwargs.pop('retrySleep', 30)
        self.errorLogThreshold = kwargs.pop('errorLogThreshold', 3)
        self.errorLogRepeat = kwargs.pop('errorLogRepeat', 3)
        self.minApiVersion = kwargs.pop("minApiVersion", 1.0)

        for key, value in kwargs.items():
            setattr(self, key, value)

        if self.errorLogRepeat <= 0:
            self.errorLogRepeat = 1
        if self.retrySleep <= 0:
            self.retrySleep = 1

        if self.log == None:
            self.log = GetLogger()

        self.downloader = HTTPDownloader(self.server, self.port, self.username, self.password)
    #pylint: enable=unused-argument

    def _CallWithRetry(self, methodName, methodParams=None, apiVersion=None, timeout=180):
        """Call a SolidFire API method, retrying on transient errors
        Arguments:
            methodName:     The method to call
            methodparams:   dictionary of parameters for the call
            apiVersion:     API endpoint version to use
            port:           the port to use
            timeout:        how long to wait for the call before abandoning the connection
        Returns:
            The API response dictionary
        """

        apiVersion = apiVersion or self.minApiVersion
        retryCount = 0
        errorCount = 0
        lastErrorMessage = ''
        while True:
            if errorCount >= self.errorLogThreshold and errorCount % self.errorLogRepeat == 0:
                self.log.error(lastErrorMessage)

            try:
                return self._Call(methodName, methodParams, apiVersion, timeout)
            except SolidFireError as ex:
                if retryCount < self.maxRetryCount and ex.IsRetryable():
                    retryCount += 1
                    errorCount += 1
                    lastErrorMessage = str(ex)
                    time.sleep(self.retrySleep)
                    continue
                raise
            except Exception as ex:
                self.log.error("_CallWithRetry general Exception: ex:{}".format(str(ex)))
                raise

    def _Call(self, methodName, methodParams=None, apiVersion=None, timeout=180):
        """Call a SolidFire API method
        Arguments:
            methodName:     The method to call
            methodparams:   dictionary of parameters for the call
            apiVersion:     API endpoint version to use
            port:           the port to use
            timeout:        how long to wait for the call before abandoning the connection
        Returns:
            The API response dictionary
        """
        methodParams = methodParams or {}
        apiVersion = apiVersion or self.minApiVersion

        endpoint = 'https://{}:{}/json-rpc/{:.1f}'.format(self.server, self.port, apiVersion)
        context = None
        try:
            # pylint: disable=no-member
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            # pylint: enable=no-member
        except AttributeError:
            pass

        api_call = json.dumps({'method': methodName, 'params': methodParams, 'id': self._GetReqid()})
        request = six.moves.urllib.request.Request(endpoint, api_call)
        request.add_header('Content-Type', 'application/json-rpc')
        request.add_header('Authorization', b"Basic " + base64.b64encode('{}:{}'.format(self.username, self.password).encode()).strip())

        self.log.debug('API call {} on {}'.format(api_call, endpoint))
        try:
            if context:
                try:
                    # pylint: disable=unexpected-keyword-arg
                    apiResponse = six.moves.urllib.request.urlopen(request, timeout=timeout, context=context)
                    # pylint: enable=unexpected-keyword-arg
                except TypeError:
                    apiResponse = six.moves.urllib.request.urlopen(request, timeout=timeout)
            else:
                apiResponse = six.moves.urllib.request.urlopen(request, timeout=timeout)
        except (socket.timeout, socket.herror, socket.gaierror) as ex:
            raise SFConnectionError(self.server, endpoint, ex, methodName, methodParams)
        except six.moves.urllib.error.HTTPError as ex:
            if ex.code == 401:
                raise UnauthorizedError.APIContext(methodName, methodParams, self.server, endpoint)
            elif ex.code == 404:
                raise SolidFireAPIError(methodName, methodParams, self.server, endpoint, 'xUnknownAPIVersion', 500, 'HTTP Error 404: Not Found - url=[{}]'.format(endpoint))
            else:
                raise SFConnectionError(self.server, endpoint, ex, methodName, methodParams)
        except six.moves.urllib.error.URLError as ex:
            if type(ex.reason) in [socket.timeout, socket.herror, socket.gaierror]:
                raise SFConnectionError(self.server, endpoint, ex.reason, methodName, methodParams)
            if type(ex.reason) == OSError:
                raise SFConnectionError(self.server, endpoint, ex.reason, methodName, methodParams)
            raise SFConnectionError(self.server, endpoint, ex, methodName, methodParams)
        except six.moves.http_client.BadStatusLine as ex:
            raise SFConnectionError(self.server, endpoint, ex, methodName, methodParams)

        responseStr = apiResponse.read()
        self.log.debug2('API response {}'.format(responseStr))
        try:
            responseJson = json.loads(responseStr)
        except ValueError as ex:
            raise SFConnectionError(self.server, endpoint, ex, methodName, methodParams)

        if 'error' in responseJson:
            raise SolidFireAPIError(methodName,
                                    methodParams,
                                    self.server,
                                    endpoint,
                                    responseJson['error']['name'],
                                    responseJson['error']['code'] if 'code' in responseJson['error'] else 500,
                                    responseJson['error']['message'] if 'message' in responseJson['error'] else "<empty message>")

        return responseJson['result']

    def _HttpDownload(self, remotePath, timeout=300):
        """Download a URL (GET) and return the content. For large binary files, see _HttpStreamingDownload
        Arguments:
            remotePath:     the path component of the URL
            port:           the port to use
            timeout:        how long to stay connected before abandoning the transfer

            The download URL will be constructed like https://self.server:port/remotePath

        Returns:
            The content retrieved from the URL
        """
        return self.downloader.Download(remotePath, useAuth=True, useSSL=True, timeout=timeout)

    def _HttpStreamingDownload(self, remotePath, localFile, timeout=300):
        """
        Download a URL (GET) to a file.  Suitable for large/binary files

        Args:
            remotePath:     the path component of the URL
            localPath:      fully qualified path to the local file to save the content in. The directory
                            component of the path must already exist
            port:           the port to use
            timeout:        how long to stay connected before abandoning the transfer

            The download URL will be constructed like https://self.server:port/remotePath
        """
        return self.downloader.StreamingDownload(remotePath, localFile, useAuth=True, useSSL=True, timeout=timeout)

    def WaitForUp(self):
        """
        Wait for the API to be up and responding
        """
        old_threshold = self.errorLogThreshold
        self.errorLogThreshold = 1000000
        try:
            self._CallWithRetry("GetAPI")
        finally:
            self.errorLogThreshold = old_threshold

    def _GetReqid(self):
        """Get next request ID"""
        rv = self._reqid
        self._reqid += 1
        return rv

#pylint: disable=method-hidden
class SolidFireClusterAPI(SolidFireAPI):
    """Make SolidFire cluster API calls
    NOT thread safe - each thread should use its own instance"""

    def __init__(self, *args, **kwargs):
        SolidFireAPI.__init__(self, *args, **kwargs)
        self._nodeIdToMipCache = {}
        self._nodeMap = {}

    def HttpDownload(self, url, timeout=300):
        return self._HttpDownload(url, timeout=timeout)

    def Call(self, methodName, methodParams=None, apiVersion=None, timeout=180):
        """Call a SolidFire Cluster API method"""
        apiVersion = apiVersion or self.minApiVersion
        result = self._Call(methodName, methodParams, apiVersion, timeout)
        if methodName == 'ListActiveNodes' or methodName == 'ListAllNodes':
            self._RefreshNodeIdToMipCache(result['nodes'])
        return result

    def CallWithRetry(self, methodName, methodParams=None, apiVersion=None, timeout=180):
        """Call a SolidFire Cluster API method"""
        apiVersion = apiVersion or self.minApiVersion
        result = self._CallWithRetry(methodName, methodParams, apiVersion, timeout)
        if methodName == 'ListActiveNodes' or methodName == 'ListAllNodes':
            self._RefreshNodeIdToMipCache(result['nodes'])
        return result

    def NodeIdToMip(self, nodeID, refresh=False):
        """Return the MIP for nodeID
        If refresh is True, then refresh the nodeID to MIP cache.
        """
        if refresh or not len(self._nodeIdToMipCache):
            self.CallWithRetry('ListActiveNodes')
        if nodeID in self._nodeIdToMipCache:
            return self._nodeMap.get(self._nodeIdToMipCache[nodeID], self._nodeIdToMipCache[nodeID])
        mip = next((nodeID for ni in self._nodeIdToMipCache if self._nodeIdToMipCache[ni] == nodeID), None)
        if mip:
            return mip
        raise UnknownNodeError("nodeID {} is not in list of active nodes".format(nodeID))

    def _RefreshNodeIdToMipCache(self, nodes):
        """Refresh nodeID to MIP cache dictionary using nodes"""
        self._nodeIdToMipCache.clear()
        for node in nodes:
            self._nodeIdToMipCache[node['nodeID']] = node['mip']

    def NodeCall(self, nodeID, methodName, methodParams=None, apiVersion=5.0, timeout=60):
        """Call a SolidFire Node API method on a node in this cluster"""
        nodeIP = self.NodeIdToMip(nodeID)
        nodeApi = SolidFireNodeAPI(nodeIP, self.username, self.password, port=442, logger=self.log, maxRetryCount=self.maxRetryCount, retrySleep=self.retrySleep, errorLogThreshold=self.errorLogThreshold, errorLogRepeat=self.errorLogRepeat)
        return nodeApi.Call(methodName, methodParams, apiVersion, timeout)

    def NodeCallWithRetry(self, nodeID, methodName, methodParams=None, apiVersion=5.0, timeout=60):
        """Call a SolidFire Node API method on a node in this cluster"""
        nodeIP = self.NodeIdToMip(nodeID)
        nodeApi = SolidFireNodeAPI(nodeIP, self.username, self.password, port=442, logger=self.log, maxRetryCount=self.maxRetryCount, retrySleep=self.retrySleep, errorLogThreshold=self.errorLogThreshold, errorLogRepeat=self.errorLogRepeat)
        return nodeApi.CallWithRetry(methodName, methodParams, apiVersion, timeout)

    def GetNodeApi(self, nodeID):
        """Returns an instance of a per-node API interface for nodeID"""
        nodeIP = self.NodeIdToMip(nodeID)
        return SolidFireNodeAPI(nodeIP, self.username, self.password, port=442, logger=self.log, maxRetryCount=self.maxRetryCount, retrySleep=self.retrySleep, errorLogThreshold=self.errorLogThreshold, errorLogRepeat=self.errorLogRepeat)

    def GetServer(self):
        """Return the hostname or IP address of the server used for cluster API calls"""
        return self.server

    def GetRetryCount(self):
        """Return the number of attempts SolidFireClusterAPI will make to an API call when there are transient errors"""
        return self.maxRetryCount

    def GetRetrySleepSeconds(self):
        """Return the number of seconds SolidFireClusterAPI waits between API call attempts when there are transient errors"""
        return self.retrySleep

    def TestConnectivity(self, ip, port, timeout=30):
        """Test that an IP/port can be connected to"""
        sock = socket.socket()
        sock.settimeout(timeout)
        try:
            sock.connect((ip, port))
            sock.close()
            return True
        except (socket.timeout, socket.error, socket.herror, socket.gaierror):
            return False
#pylint: enable=method-hidden

class SolidFireBootstrapAPI(SolidFireAPI):
    """Make calls to the bootstrap API on SolidFireNodes
    NOT thread safe - each thread should use its own instance"""

    def __init__(self, nodeIP):
        super(SolidFireBootstrapAPI, self).__init__(server=nodeIP,
                                                    username=None,
                                                    password=None,
                                                    port=443)

    def Call(self, methodName, methodParams=None, timeout=120, apiVersion=1.0):
        """
        Call a bootstrap method

        Args:
            methodName:     the method to call
            methodParams:   the parameters to use
            timeout:        the timeout for the call
        """
        return self._Call(methodName, methodParams, apiVersion=apiVersion, timeout=timeout)

    def GetBootstrapConfig(self):
        """
        Get the current discovered bootstrap config

        Returns:
            A dictionary of config info
        """
        return self.Call("GetBootstrapConfig")

    def GetBootstrapNodes(self):
        """
        Get a list of node IPs discovered by the bootstrapper

        Returns:
            A list of node IP addresses
        """
        return self.Call("GetBootstrapConfig", apiVersion=1.0)["nodes"]

    def CreateCluster(self, mvip, svip, username, password):
        """
        Create a cluster

        Args:
            mvip:       the MVIP of the new cluster
            svip:       the SVIP of the new cluster
            username:   the name for the first cluster admin user
            password:   the password for the first cluster admin
        """
        params = {}
        params["mvip"] = mvip
        params["svip"] = svip
        params["username"] = username
        params["password"] = password
        params["nodes"] = self.GetBootstrapNodes()
        params["acceptEula"] = True
        if len(params["nodes"]) == 1:
            bc = self.Call("GetBootstrapConfig", apiVersion=9.0)
            if bc["nodes"][0]["nodeType"] != "SFDEMO":
                self.CreateStandaloneCluster(mvip, svip, username, password)
                return

        self.Call("CreateCluster", params)

    def CreateStandaloneCluster(self, mvip, svip, username, password):
        """
        Create a single node cluster

        Args:
            mvip:       the MVIP of the new cluster
            svip:       the SVIP of the new cluster
            username:   the name for the first cluster admin user
            password:   the password for the first cluster admin
        """
        params = {}
        params["mvip"] = mvip
        params["svip"] = svip
        params["username"] = username
        params["password"] = password
        params["acceptEula"] = True
        params["nodes"] = self.GetBootstrapNodes()
        self.Call("CreateStandaloneCluster", params, apiVersion=6.0)

class AutotestAPI(SolidFireAPI):
    """Make AT2 API calls
    NOT thread safe - each thread should use its own instance"""

    def __init__(self,
                 server="autotest2.solidfire.net",
                 username="automation",
                 password="password",
                 port=443,
                 **kwargs):
        super(AutotestAPI, self).__init__(server, username, password, **kwargs)

    def ListResources(self):
        """
        Get a list of all known AT2 resources

        Returns:
            A dictionary of AT2 resources (dict)
        """
        return self._Call("ListResources")

    def ListNetworks(self):
        """
        Get a list of 'network' resources from AT2
        """
        return self._Call("ListResources", {"resourceTypeName" : "network"}, apiVersion=2.0)["resources"]

    def ListFDVAPool(self):
        """
        Get a list of FDVA resources from AT2
        """
        return self._Call("ListResources", {"resourceTypeName" : "fdva"}, apiVersion=2.0)["resources"]

    def ListNodePool(self):
        return self._Call("ListNodePool")["nodes"]

    def ListClientPool(self):
        return self._Call("ListClientPool")["clients"]

#pylint: disable=method-hidden
class SolidFireNodeAPI(SolidFireAPI):
    """Make SolidFire node API calls
    NOT thread safe - each thread should use its own instance"""

    def __init__(self, nodeIP, username=None, password=None, port=442, **kwargs):
        super(SolidFireNodeAPI, self).__init__(nodeIP, username, password, port, **kwargs)
        self.minApiVersion = 5.0

    def Call(self, methodName, methodParams=None, apiVersion=None, timeout=60):
        """Call a SolidFire Node API method"""
        apiVersion = apiVersion or self.minApiVersion
        return self._Call(methodName, methodParams, apiVersion, timeout)

    def CallWithRetry(self, methodName, methodParams=None, apiVersion=None, timeout=60):
        """Call a SolidFire Node API method"""
        apiVersion = apiVersion or self.minApiVersion
        return self._CallWithRetry(methodName, methodParams, apiVersion, timeout)

    def GetAllRtfiStatus(self):
        """
        Get the entire history of status from the RTFI status server

        Returns:
            A list of dictionaries of status (list of dict) or None
        """
        text = self._HttpDownload("/config/rtfi/status/all.json", timeout=10)
        try:
            return json.loads(text)
        except ValueError:
            raise SolidFireError("Could not decode status JSON: {}".format(text))

    def GetLatestRtfiStatus(self):
        """
        Get the latest status from the RTFI status server

        Returns:
            A dictionary containing the latest RTFI status (dict) or None
        """
        text = self._HttpDownload("/config/rtfi/status/current.json", timeout=10)
        try:
            return json.loads(text)
        except ValueError:
            raise SolidFireError("Could not decode status JSON: {}".format(text))

    def GetRtfiLog(self, localFile):
        """
        Get the latest RTFI log from the node

        Args:
            localFile:      path to save the log file
        """
        self._HttpStreamingDownload("/config/rtfi/status/rtfi.log", localFile, timeout=60)

    def CreateSupportBundle(self, bundleName, localPath=None, extraArgs=None):
        """Create a support bundle on this node, and optionally download it locally

        Arguments:
            bundleName:     the name of the support bundle
            localPath:      the local directory to download the bundle to.  If None, the
                            bundle will be left on the node and not downloaded
            extraArgs:      extra args to pass to the support bundle script
        """

        # Create the support bundle
        params = {'bundleName': bundleName, 'extraArgs' : ''}
        if extraArgs:
            params['extraArgs'] = extraArgs
        if '--binary' not in params['extraArgs'] and '-b' not in params['extraArgs']:
            params['extraArgs'] += ' --binary'

        result = self.CallWithRetry('CreateSupportBundle', params, apiVersion=7.3, timeout=600)

        if localPath:
            # Create the path if it doesn't exist
            if not os.path.exists(localPath):
                try:
                    os.mkdir(localPath)
                except OSError as ex:
                    if ex.errno != 17: # Already exists
                        raise
            if not os.path.isdir(localPath):
                pass
            remoteFileName = result['details']['files'][0]
            remoteURL = result['details']['url'][0]

            # Get the path component of the URL
            pieces = six.moves.urllib.parse.urlparse(remoteURL)
            remotePath = pieces.path.lstrip('/')

            localFileName = os.path.join(localPath, remoteFileName)

            # Download the bundle
            self._HttpStreamingDownload(remotePath, localFileName, timeout=900)
            return localFileName

        # Caller did not pass in localPath
        return None
#pylint: enable=method-hidden

class SSHConnection(object):
    """Helper class for making SSH connections and running commands on nodes/clients"""

    def __init__(self, ipAddress, username, password, keyfile=None):
        """
        Constructor

        Args:
            ipAddress:      the address of the server
            username:       the username to use to connect
            password:       the password to use to connect
            keyfile:        filename of RSA key to use to connect
        """
        self.ipAddress = ipAddress
        self.username = username
        self.password = password
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.load_system_host_keys()
        self.keyfile = None
        self.log = GetLogger()

        # Try to find a default keyfile on Windows
        if not keyfile and sys.platform.startswith("win"):
            self.keyfile = os.environ["HOMEDRIVE"] + os.environ["HOMEPATH"] + "\\ssh\\id_rsa"
            if not os.path.exists(keyfile):
                self.keyfile = None

    def __enter__(self):
        """Create the SSH connection"""
        self.Connect()
        return self

    def __exit__(self, extype, value, traceback):
        """Close the SSH connection"""
        self.Close()

    def __del__(self):
        """Cleanup the SSH connection"""
        self.Close()

    def Connect(self):
        """
        Create the SSH connection
        """
        try:
            self.client.connect(self.ipAddress, username=self.username, password=self.password, key_filename=self.keyfile)
            return self
        except paramiko.AuthenticationException:
            # If a password was given, try again without the keyfile
            if self.keyfile and self.password:
                try:
                    self.client.connect(self.ipAddress, username=self.username, password=self.password)
                    return self
                except paramiko.AuthenticationException:
                    pass
            raise UnauthorizedError.IPContext(self.ipAddress)
        except paramiko.SSHException as e:
            raise SolidFireError("SSH error connecting to {}: {}".format(self.ipAddress, e))
        except socket.error as e:
            raise SFConnectionError(self.ipAddress, self.ipAddress, e, message="Could not connect")

    def IsAlive(self):
        """
        Check if the SSH session is still valid

        Returns:
            Boolean true if the session is alive, false otherwise
        """
        return self.client != None and self.client.get_transport() != None and self.client.get_transport().is_active()

    def Close(self):
        """
        Close the SSH connection
        """
        if self.client:
            self.client.close()
            self.client = None

    def RunCommand(self, command, exceptOnError=True, pipeFail=True):
        """
        Run a command on the remote host

        Args:
            command:        the command to run
            exceptOnError:  raise a SolidFireError if the command's return code is non-zero

        Returns:
            A tuple of (return code, stdout, stderr)
        """
        if not self.client:
            raise SolidFireError("SSH session is not connected")

        self.log.debug2("Executing remote command=[{}] on host={}".format(command, self.ipAddress))
        if pipeFail:
            cmd = "set -o pipefail; {}".format(command)
        else:
            cmd = command
        _, stdout, stderr = self.client.exec_command(cmd)
        retcode = stdout.channel.recv_exit_status()
        stdout_data = "".join(stdout.readlines())
        stderr_data = "".join(stderr.readlines())

        self.log.debug2("retcode=[{}] stdout=[{}] stderr=[{}] host=[{}]".format(retcode, stdout_data.rstrip("\n"), stderr_data.rstrip("\n"), self.ipAddress))

        if retcode != 0 and exceptOnError:
            raise SolidFireError("SSH command failed: command=[{}] stderr=[{}]".format(command, stderr_data))
        return (retcode, stdout_data, stderr_data)

    def PutFile(self, localPath, remotePath):
        """
        Copy a file to the remote host

        Args:
            localPath:  the path to the file on the local system
            remotePath: the remote path to copy the file to
        """
        if not self.client:
            raise SolidFireError("SSH session is not connected")

        self.log.debug2("Copying localPath=[{}] to host={} remotePath=[{}]".format(localPath, self.ipAddress, remotePath))
        try:
            sftp = self.client.open_sftp()
            sftp.put(localPath, remotePath)
            sftp.close()
        except paramiko.SSHException as e:
            raise SolidFireError("SFTP error connecting to {}: {}".format(self.ipAddress, e))


def GetHighestAPIVersion(mvip, username, password):
    """
    Get the highest API version a cluster supports

    Returns:
        A floating point API version
    """
    api = SolidFireClusterAPI(mvip,
                              username,
                              password,
                              maxRetryCount=5,
                              retrySleep=20,
                              errorLogThreshold=1,
                              errorLogRepeat=1)
    result = api.CallWithRetry("GetAPI", {}, apiVersion=1.0)
    try:
        return max([float(ver) for ver in result['supportedVersions']])
    except KeyError: # Pre-boron did not have this key in GetAPI
        return 4.0
    except ValueError: # Format must have changed, assume an early version
        return 5.0




#pylint: enable=unidiomatic-typecheck,protected-access,global-statement
