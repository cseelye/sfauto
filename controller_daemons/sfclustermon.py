import sys
import json
import time
import re
import libsf
from libsf import mylog
import MySQLdb;

ssh_user = "root"                   # The SSH username for the nodes
ssh_pass = "password"              # The SSH password for the nodes

import logging
mylog.console.setLevel(logging.DEBUG)

class ClusterToMonitor:
    def __init__(self):
        self.mvip = ""
        self.username = ""
        self.password = ""
        self.ignore_faults = set()

    def __dir__(self):
        return ["mvip", "username", "password", "ignore_faults"]

class NodeHealth:
    def __init__(self):
        self.ip = ""
        self.hostname = ""
        self.corefiles = False
        self.responsive = True

class ClusterHealth:
    def __init__(self):
        self.mvip = ""
        self.name = ""
        self.lastCheckTimestamp = time.time()
        self.mvipUp = True
        self.xUnknownBlockID = False
        self.currentClusterFaults = set()
        self.nodes = []
    
    def __dir__(self):
        return ["mvip", "lastCheckTimestamp", "unresponsiveNodes", "mvipUp", "coreFiles", "xUnknownBlockID", "currentClusterFaults"]
    
    def IsHealthy(self):
        if not self.mvipUp: 
            mylog.debug("Unhealthy because MVIP is not up")
            return False
        else:
            mylog.debug("MVIP is up")
        if self.xUnknownBlockID: 
            mylog.debug("Unhealthy because xUnknownBlockID")
            return False
        else:
            mylog.debug("no xUnknownBlockID")
        if len(self.currentClusterFaults) > 0:
            mylog.debug("Unhealthy because of cluster faults")
            return False
        else:
            mylog.debug("There are no cluster faults")
        for node in self.nodes:
            if not node.responsive:
                mylog.debug("Unhealthy because " + node.ip + " is not responding")
                return False
            else:
                mylog.debug(node.ip + " is responding")
            if node.corefiles:
                mylog.debug("Unhealthy because " + node.ip + " has core files")
                return False
            else:
                mylog.debug(node.ip + " has no core files")
        
        mylog.debug("Cluster is healthy")
        return True
    
    def Severity(selfself):
        return "error"
#        if len(self.unresponsiveNodes) > 0: return "warn"
#        if not self.mvipUp: return "error"
#        if self.xUnknownBlockID: return "error"
#        if len(self.currentClusterFaults) > 0: return "error"
#        if self.coreFiles: return "warn"
    
    def IsSameHealthAs(self, otherClusterHealth):
        for item in dir(otherClusterHealth):
            if item == "lastCheckTimestamp": continue
            if getattr(self, item) != getattr(otherClusterHealth, item): return False
        return True
    
    def EmailSummary(self):
        summary = ""
        if not self.mvipUp:
            summary += "\nMVIP is not responding to ping"
        if self.xUnknownBlockID:
            summary += "\nxUnknownBlockID detected"
        if len(self.currentClusterFaults) > 0:
            summary += "\nUnresolved cluster faults detected: " + ", ".join(self.currentClusterFaults)
        for node in self.nodes:
            if not node.responsive: summary += "\n" + node.ip + " is not responding"
            if node.corefiles: summary += "\n" + node.ip + " has core files"
        
        return summary.lstrip()

def GetConfig():
    config_file = "sfclustermon.json"
    mylog.info("Reading config file " + config_file)
    config_lines = ""
    with open(config_file, "r") as config_handle:
        config_lines = config_handle.readlines()

    # Remove comments from JSON before loading it
    new_config_text = ""
    for line in config_lines:
        line = re.sub("(//.*)", "", line)
        if re.match("^\s*$", line): continue
        new_config_text += line
    mylog.debug("Parsing JSON:\n" + new_config_text)
    config_json = json.loads(new_config_text)

    # Make sure all required params are present
    required_keys = ["monitor_interval", "clusters"]
    error = False
    for key in required_keys:
        if not key in config_json.keys():
            mylog.error("Missing required key '" + key + "' from config file")
            error = True
    if error: exit(1)
    
    error = False
    clusters_from_config = dict()
    for cluster_mvip in config_json["clusters"].keys():
        required_keys = ["username", "password"]
        for key in required_keys:
            if not key in config_json["clusters"][cluster_mvip]:
                mylog.error("Missing required key '" + key + "' from cluster " + cluster_mvip + " in config file")
                error = True
        if not isinstance(config_json["clusters"][cluster_mvip]["ignore_faults"], list):
            mylog.error("ignore_faults key for cluster " + cluster_mvip + " must be a list")
            error = True
        if error: continue

        cluster = ClusterToMonitor()
        cluster.mvip = cluster_mvip
        cluster.username = config_json["clusters"][cluster_mvip]["username"]
        cluster.password = config_json["clusters"][cluster_mvip]["password"]
        cluster.ignore_faults = set(config_json["clusters"][cluster_mvip]["ignore_faults"])
        clusters_from_config[cluster_mvip] = cluster
            
    if error: sys.exit(1)
    
    return config_json["monitor_interval"], clusters_from_config

def main():
    monitor_interval = 60
    clusters_to_monitor = dict()

    # Connect to database
    try:
        db = MySQLdb.connect(host="localhost", user="root", passwd="password", db="monitor")
    except MySQLdb.Error as e:
        print "Error " + str(e.args[0]) + ": " + str(e.args[1])
        sys.exit(1)
    db_cursor = db.cursor()

    while True:

        # Read configuration file
        new_monitor_interval, new_clusters_to_monitor = GetConfig()
        
        # See if monitor interval has changed
        if new_monitor_interval != monitor_interval:
            mylog.info("  New monitor interval = " + str(new_monitor_interval) + " seconds")
        monitor_interval = new_monitor_interval
        
        # See if any clusters have been added/updated
        for new_mvip, new_cluster in new_clusters_to_monitor.iteritems():
            if new_mvip in clusters_to_monitor:
                old_cluster = clusters_to_monitor[new_mvip]
                for item in dir(new_cluster):
                    if getattr(new_cluster, item) != getattr(old_cluster, item):
                        mylog.info("  Updating cluster " + new_cluster.mvip + " " + item + " to " + str(getattr(new_cluster, item)))
                break
            else:
                mylog.info("  Adding new cluster to monitor:")
                for item in dir(new_cluster):
                    mylog.info("    " + item + " = " + str(getattr(new_cluster, item)))
        
        # See if any clusters have been removed
        for old_mvip in clusters_to_monitor.keys():
            if old_mvip not in new_clusters_to_monitor.keys():
                mylog.info("Stopping monitoring cluster " + old_mvip)
        
        clusters_to_monitor = new_clusters_to_monitor


        # Check the health of each cluster
        for cluster in clusters_to_monitor.values():
            mylog.info("Checking the health of cluster " + cluster.mvip)
            current_health = ClusterHealth()
            #current_health.__new__()
            current_health.mvip = cluster.mvip

            # Make sure MVIP is up
            mylog.info("  Checking MVIP status")
            if not libsf.Ping(cluster.mvip):
                mylog.error("  MVIP " + cluster.mvip + " is not responding to ping")
                current_health.mvipUp = False
                current_healthcheck[cluster.mvip] = current_health
                continue

            # get the list of nodes in the cluster
            mylog.info("  Getting cluster info")
            obj = libsf.CallApiMethod(cluster.mvip, cluster.username, cluster.password, "GetClusterInfo", {}, ExitOnError=False)
            current_health.name = obj["clusterInfo"]["name"]
            
            mylog.info("  Getting a list of nodes in the cluster")
            node_ips = []
            obj = libsf.CallApiMethod(cluster.mvip, cluster.username, cluster.password, "ListActiveNodes", {}, ExitOnError=False)
            for node in obj["nodes"]:
                node_ips.append(node["mip"])
            node_ips.sort()
            
            # Make sure all nodes are up and check for cores on each node
            mylog.info("  Checking that all nodes are up")
            for node_ip in node_ips:
                node = NodeHealth()
                node.ip = node_ip
                
                if libsf.Ping(node_ip):
                    node.responsive = True
                    mylog.debug("  " + node_ip + " is responding to ping")
                    core_count = libsf.CheckCoreFiles(node_ip, ssh_user, ssh_pass)
                    if core_count > 0:
                        node.corefiles = True
                        mylog.error("  Found " + str(core_count) + " core files on node " + node_ip)
                else:
                    node.responsive = False
                    mylog.error("  Node " + node_ip + " is not responding to ping")
                
                current_health.nodes.append(node)
            
            # Look for new cluster faults
            mylog.info("  Checking for unresolved cluster faults")
            if len(cluster.ignore_faults) > 0: mylog.info("  If these faults are present, they will be ignored: " + ", ".join(cluster.ignore_faults))
            obj = libsf.CallApiMethod(cluster.mvip, cluster.username, cluster.password, "ListClusterFaults", {"exceptions": 1, "faultTypes": "current"})
            if (len(obj["faults"]) > 0):
                current_faults = set()
                for fault in obj["faults"]:
                    if fault["code"] not in current_faults:
                        current_faults.add(fault["code"])
                
                # Remove the whitelisted faults
                relevent_faults = current_faults.difference(cluster.ignore_faults)

                if len(relevent_faults) <= 0:
                    mylog.info("  Cluster faults found: " + ", ".join(current_faults))
                else:
                    current_health.currentClusterFaults = current_faults
                    mylog.error("  Cluster faults found: " + ", ".join(current_faults))
            
            # Look for xUnknownBlockID
            mylog.info("  Checking for errors in cluster event log")
            if libsf.CheckForEvent("xUnknownBlockID", cluster.mvip, cluster.username, cluster.password):
                mylog.error("  Found xUnknownBlockID in event log!")
                current_health.xUnknownBlockID = True
            
            # Health message
            ishealthy = 1
            message = ""
            
            # Convert bool
            if not current_health.IsHealthy():
                ishealthy = 0
            
            # Update the database
            sql = """
                INSERT INTO clusters
                    (
                        `name`,
                        `mvip`,
                        `ishealthy`,
                        `current_faults`,
                        `message`,
                        `timestamp`
                    )
                    VALUES
                    (
                        '""" + str(current_health.name) + """',
                        '""" + str(current_health.mvip) + """',
                        '""" + str(ishealthy) + """',
                        '""" + ",".join(current_health.currentClusterFaults) + """',
                        '""" + str(current_health.EmailSummary().strip()) + """',
                        '""" + str(time.time()) + """'
                    )
                    ON DUPLICATE KEY UPDATE
                        `mvip`='""" + str(current_health.mvip) + """',
                        `ishealthy`='""" + str(ishealthy) + """',
                        `current_faults`='""" + ",".join(current_health.currentClusterFaults) + """',
                        `message`='""" + str(current_health.EmailSummary().strip()) + """',
                        `timestamp`='""" + str(time.time()) + """'
            """
            print sql + "\n"
            try:
                db_cursor.execute(sql)
            except MySQLdb.Error as e:
                mylog.error("Error " + str(e.args[0]) + ": " + str(e.args[1]))
                pass

        # Wait for monitor_interval seconds
        mylog.info("Monitor waiting for " + str(monitor_interval) + " seconds before checking again")
        time.sleep(monitor_interval)

if __name__ == '__main__':
    #import logging
    #mylog.console.setLevel(logging.DEBUG)
    mylog.debug("Starting " + str(sys.argv))
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
