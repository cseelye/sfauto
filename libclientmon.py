import sys
import MySQLdb;

class ClientMonError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

class VmInfo:
    def __init__(self):
        self.Hostname = ""
        self.IpAddress = ""
        self.MacAddress = ""
        self.VdbenchCount = -1
        self.VdbenchExit = -1
        self.CpuUsage = 0
        self.MemUsage = 0
        self.Group = ""
        self.Timestamp = 0

class SfautoClientMon:
    __db = None
    __cursor = None
    
    def __init__(self):
        try:
            self.__db = MySQLdb.connect(host="172.25.107.4", user="root", passwd="password", db="monitor")
        except MySQLdb.Error as e:
            raise ClientMonError("Error " + str(e.args[0]) + ": " + str(e.args[1]))
        self.__cursor = self.__db.cursor(MySQLdb.cursors.DictCursor)
    
    def GetVmIpFromName(self, VmName):
        
        sql = "SELECT ip FROM clients WHERE `hostname`='" + VmName + "'"
        self.__cursor.execute(sql)
        row = self.__cursor.fetchone()
        return row["ip"]

    def GetVmIpFromMac(self, VmMac):
        mac = VmMac.replace(":", "")
        
        sql = "SELECT ip FROM clients WHERE `mac`='" + mac + "'"
        self.__cursor.execute(sql)
        row = self.__cursor.fetchone()
        return row["ip"]
    
    def GetGroupVmInfo(self, GroupName = ""):
        vm_list = []
        sql = "SELECT * FROM clients WHERE `group`='" + GroupName + "'"
        self.__cursor.execute(sql)
        results = self.__cursor.fetchall()
        for row in results:
            vm = VmInfo()
            vm.Hostname = row["hostname"]
            vm.IpAddress = row["ip"]
            vm.MacAddress = row["mac"]
            vm.VdbenchCount = row["vdbench_count"]
            vm.VdbenchExit = row["vdbench_last_exit"]
            vm.CpuUsage = row["cpu_usage"]
            vm.MemUsage = row["mem_usage"]
            vm.Group = row["group"]
            vm.Timestamp = row["timestamp"]
            vm_list.append(vm)
        return vm_list




