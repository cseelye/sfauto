
import sys
import re
import suds
from suds.client import Client
sys.path.append("..")
from lib.libsf import mylog

#from sf_platform.core.CommonExceptions import BaseError
#from sf_platform.core.log import get_sflogging
#_log = get_sflogging(__name__)
from lib.libsf import SfError as BaseError
_log = mylog

class QmetryError(BaseError):
    ROBOT_EXIT_ON_FAILURE = True
    def __init__(self, value, innerException=None):
        self.value = value
        self.innerException = None
    def __str__(self):
        return str(self.value)

class QmetryClient(object):
    """ Access to Qmetry test suites """

    DEFAULT_URL = "http://solidfire.qmetry.com/qmetryapp/WEB-INF/ws/service.php?wsdl"
    DEFAULT_USERNAME = "autouser"
    DEFAULT_PASSWORD = "1f@stSSD"

    def __init__(self, soapURL=DEFAULT_URL, username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD):
        self.soapURL = soapURL
        self.username = username
        self.password = password

        _log.debug("Connecting to Qmetry at " + self.soapURL)
        try:
            self.client = Client(self.soapURL, cachingpolicy=1)
            self.authToken = self.client.service.login(self.username, self.password)
        except suds.WebFault as e:
            self.client = None
            raise QmetryError(e.message, innerException=e)
        except Exception as e:
            self.client = None
            raise QmetryError(e.message, innerException=e)

    def __del__(self):
        if self.client:
            _log.debug("Logging out of Qmetry")
            self.client.service.logout(self.authToken)
            self.client = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__del__()

    def SetScope(self, projectName, releaseName, buildName):
        """
        Set the current scope of this Qmetry client
        """
        _log.debug("Changing scope to " + projectName + "/" + releaseName + "/" + buildName)
        try:
            self.client.service.setScope(self.authToken, projectName, releaseName, buildName)
        except suds.WebFault as e:
            raise QmetryError(e.message, innerException=e)


    def FindSuiteFromPath(self, suitePath):
        """
        Get a suite reference from a path string
        """
        # Split the name into a list of folders with the suite at the end
        pieces = re.split(r"[\/]+", suitePath.strip("/").strip(r"\\"))
        suite_name = pieces.pop()

        # Walk down the tree and find each folder
        parentFolderID = 0
        for folder_name in pieces:
            if not folder_name:
                continue
            folder = self.FindSuiteFolder(folder_name, parentFolderID)
            parentFolderID = folder.id

        # Find the suite
        suite = self.FindSuite(suite_name, parentFolderID)
        return suite

    def FindSuiteFolder(self, folderName, parentFolderID):
        """
        Get a test suite folder reference from a name and parent ID
        """
        subfolders = self.client.service.listFoldersFromParentId(self.authToken, "testsuite", parentFolderID)
        for fold in subfolders:
            if fold.name.lower() == folderName.lower():
                return fold
        raise QmetryError("Could not find folder '" + folderName + "'")

    def FindSuite(self, suiteName, parentFolderID):
        """
        Get a suite reference from a name and parent folder ID
        """
        suites = self.client.service.listTestSuitesFromFolderId(self.authToken, parentFolderID)
        for suite in suites:
            if suite.name.lower() == suiteName.lower():
                return suite
        raise QmetryError("Could not find suite '" + suiteName + "'")

    def FindPlatformInSuite(self, platformName, parentSuiteID):
        """
        Get a platform reference from a name and parent suite ID
        """
        platforms = self.client.service.listPlatformsByTestSuite(self.authToken, parentSuiteID)
        for plat in platforms:
            if plat.name.lower() == platformName.lower():
                return plat
        raise QmetryError("Could not find platform '" + platformName + "'")

    def FindTestInSuite(self, suite, platform, testCaseName):
        """
        Get a test from a suite and platform
        """
        test_list = self.ListTestsInSuite(suite, platform)
        test_case = None
        for tc in test_list:
            if tc.testcasename.lower() == testCaseName.lower():
                return tc
        raise QmetryError("Could not find test case '" + testCaseName + "'")

    def ListTestsInSuite(self, suite, platform):
        """
        Get a list of tests from a suite and platform
        """
        run_details_list = self.client.service.getTestSuiteStatusByPlatform(self.authToken, suite.id, platform.id)
        run_details = run_details_list[0]
        tc_run_details_list = run_details.details
        return tc_run_details_list

    def GetPossibleTestStatus(self):
        """
        Get a list of the accepted test statuses
        """
        status_object_list = self.client.service.listStatuses(self.authToken)
        return [str(s["statusname"]) for s in status_object_list]

    def SetTestCaseExecutionStatus(self, suitePath, platformName, testCaseName, testCaseStatus, testCaseComments = None):
        """
        Set the execution status of a test in a test suite and platform
        """
        suite = self.FindSuiteFromPath(suitePath)
        platform = self.FindPlatformInSuite(platformName, suite.id)
        test_case = self.FindTestInSuite(suite, platform, testCaseName)

        self.SetTestCaseExecutionStatusByID(suite.id, platform.id, test_case.testcaseid, testCaseStatus, testCaseComments)

    def SetTestCaseExecutionStatusByID(self, suiteID, platformID, testCaseID, testCaseStatus, testCaseComments = None):
        """
        Set the execution status of a test in a test suite and platform
        """
        if testCaseComments:
            self.client.service.executeTestCaseWithComments(self.authToken, suiteID, platformID, testCaseID, testCaseStatus, testCaseComments)
        else:
            self.client.service.executeTestCase(self.authToken, suiteID, platformID, testCaseID, testCaseStatus)

