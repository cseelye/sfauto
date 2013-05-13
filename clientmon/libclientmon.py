import MySQLdb
import time
import json
import re
import os
import sys
sys.path.append("..")
import lib.libsf
from lib.libsf import SfError

class ClientStatus:
    def __init__(self):
        self.MacAddress = ""
        self.Hostname = ""
        self.IpAddress = ""
        self.CpuUsage = -1
        self.MemUsage = -1
        self.VdbenchCount = 0
        self.VdbenchExit = -1
        self.GroupName = ""
        self.Timestamp = time.time()


class ClientMon:
    __configFile = "sfclientmon.json"
    __ipFilter = None

    def __init__(self, DbServer=None, DbUser=None, DbPass=None, DbName=None, IpFilter=None):
        config_file = self.__readconfig()
        if not DbServer: DbServer = config_file["server"]
        if not DbUser: DbUser = config_file["username"]
        if not DbPass: DbPass = config_file["password"]
        if not DbName: DbName = config_file["database"]
        if not IpFilter: IpFilter = config_file["ipFilter"]

        try:
            self.__db = MySQLdb.connect(host=DbServer, user=DbUser, passwd=DbPass, db=DbName)
        except MySQLdb.Error as e:
            raise SfError("Database error " + str(e.args[0]) + ": " + str(e.args[1]))
        self.__db_cursor = self.__db.cursor()

    def __del__(self):
        if self.__db:
            self.__db.close()

    def __readconfig(self):
        my_dir = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(my_dir, self.__configFile)

        config_lines = ""
        with open(config_path, "r") as config_handle:
            config_lines = config_handle.readlines()
        new_config_text = ""
        for line in config_lines:
            line = re.sub("(//.*)", "", line)
            if re.match("^\s*$", line): continue
            new_config_text += line
        try:
            config_json = json.loads(new_config_text)
        except ValueError as e:
            raise SfError("Error parsing config file: " + str(e))
        return config_json

    def GetClientStatus(self, MacAddress=None, IpAddress=None):
        sql = """
        SELECT
            `mac`,
            `hostname`,
            `ip`,
            `cpu_usage`,
            `mem_usage`,
            `vdbench_count`,
            `vdbench_last_exit`,
            `group`,
            `timestamp`
        FROM clients"""

        if MacAddress:
            sql += " WHERE `mac`='" + str(MacAddress) + "'"
        elif IpAddress:
            sql += " WHERE `ip`='" + str(IpAddress) + "'"

        cursor = self.__db.cursor(MySQLdb.cursors.DictCursor)
        data = None
        try:
            cursor.execute(sql)
            data = cursor.fetchone()
        except MySQLdb.Error as e:
            raise SfError("Database error " + str(e.args[0]) + ": " + str(e.args[1]))

        if not data:
            return None

        status = ClientStatus()
        status.MacAddress = str(data["mac"])
        status.Hostname = str(data["hostname"])
        status.IpAddress = str(data["ip"])
        status.CpuUsage = float(data["cpu_usage"])
        status.MemUsage = float(data["mem_usage"])
        status.VdbenchCount = int(data["vdbench_count"])
        status.VdbenchExit = int(data["vdbench_last_exit"])
        status.GroupName = str(data["group"])
        status.Timestamp = float(data["timestamp"])

        return status

    def ListAllClientStatus(self):
        sql = """
        SELECT
            `mac`,
            `hostname`,
            `ip`,
            `cpu_usage`,
            `mem_usage`,
            `vdbench_count`,
            `vdbench_last_exit`,
            `group`,
            `timestamp`
        FROM clients
        """

        status_list = []
        cursor = self.__db.cursor(MySQLdb.cursors.DictCursor)
        try:
            cursor.execute(sql)
            data = cursor.fetchone()
            while data:
                status = ClientStatus()
                status.MacAddress = data["mac"]
                status.Hostname = data["hostname"]
                status.IpAddress = data["ip"]
                status.CpuUsage = data["cpu_usage"]
                status.MemUsage = data["mem_usage"]
                status.VdbenchCount = data["vdbench_count"]
                status.VdbenchExit = data["vdbench_last_exit"]
                status.GroupName = data["group"]
                status.Timestamp = data["timestamp"]
                status_list.append(status)
                data = cursor.fetchone()
        except MySQLdb.Error as e:
            raise SfError("Database error " + str(e.args[0]) + ": " + str(e.args[1]))
        return status_list

    def ListClientStatusByGroup(self, GroupName):
        sql = """
        SELECT
            `mac`,
            `hostname`,
            `ip`,
            `cpu_usage`,
            `mem_usage`,
            `vdbench_count`,
            `vdbench_last_exit`,
            `group`,
            `timestamp`
        FROM clients
        WHERE `group`='""" + str(GroupName) + """'
        """

        status_list = []
        cursor = self.__db.cursor(MySQLdb.cursors.DictCursor)
        try:
            cursor.execute(sql)
            data = cursor.fetchone()
            while data:
                status = ClientStatus()
                status.MacAddress = data["mac"]
                status.Hostname = data["hostname"]
                status.IpAddress = data["ip"]
                status.CpuUsage = data["cpu_usage"]
                status.MemUsage = data["mem_usage"]
                status.VdbenchCount = data["vdbench_count"]
                status.VdbenchExit = data["vdbench_last_exit"]
                status.GroupName = data["group"]
                status.Timestamp = data["timestamp"]
                status_list.append(status)

                data = cursor.fetchone()
        except MySQLdb.Error as e:
            raise SfError("Database error " + str(e.args[0]) + ": " + str(e.args[1]))
        return status_list

    def UpdateClientStatus(self, MacAddress, Hostname, IpAddress, CpuUsage=None, MemUsage=None, VdbenchCount=None, VdbenchExit=None, GroupName=None, Timestamp=None):
        # Skip template VMs
        if "template" in Hostname: return
        if "gold" in Hostname: return

        # Skip this update if it does not match our filter
        if self.__ipFilter and not str(IpAddress).startswith(self.__ipFilter):
            #print "Skipping " + str(IpAddress)
            return

        if CpuUsage == None: CpuUsage = -1
        if MemUsage == None: MemUsage = -1
        if VdbenchCount == None: VdbenchCount = 0
        if VdbenchExit == None: VdbenchExit = -1
        if GroupName == None: GroupName = ""
        if Timestamp == None: Timestamp = time.time()

        sql = """
        INSERT INTO clients
            (
                `mac`,
                `hostname`,
                `ip`,
                `cpu_usage`,
                `mem_usage`,
                `vdbench_count`,
                `vdbench_last_exit`,
                `timestamp`,
                `group`
            )
            VALUES
            (
                '""" + str(MacAddress) + """',
                '""" + str(Hostname) + """',
                '""" + str(IpAddress) + """',
                '""" + str(CpuUsage) + """',
                '""" + str(MemUsage) + """',
                '""" + str(VdbenchCount) + """',
                '""" + str(VdbenchExit) + """',
                '""" + str(Timestamp) + """',
                '""" + str(GroupName) + """'
            )
            ON DUPLICATE KEY UPDATE
                `hostname`='""" + str(Hostname) + """',
                `ip`='""" + str(IpAddress) + """',
                `cpu_usage`='""" + str(CpuUsage) + """',
                `mem_usage`='""" + str(MemUsage) + """',
                `vdbench_count`='""" + str(VdbenchCount) + """',
                `vdbench_last_exit`='""" + str(VdbenchExit) + """',
                `timestamp`='""" + str(Timestamp) + """',
                `group`='""" + str(GroupName) + """'
        """
        #print sql

        try:
            self.__db_cursor.execute(sql)
        except MySQLdb.Error as e:
            raise SfError("Database error " + str(e.args[0]) + ": " + str(e.args[1]))

