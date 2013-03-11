import sys
# little hack to get to libs in the parent directory
sys.path.insert(0, "..")
import json
import time
import re
import libsf
from libsf import mylog

ssh_user = "root"                   # The SSH username for the nodes
ssh_pass = "password"               # The SSH password for the nodes

class ClusterToMonitor:
    def __init__(self):
        self.mvip = ""
        self.username = ""
        self.password = ""
        self.renotify = 60
        self.notify = []
        self.ignore_faults = set()

    def __dir__(self):
        return ["mvip", "username", "password", "notify", "ignore_faults", "renotify"]

class ClusterHealth:
    def __init__(self):
        self.mvip = ""
        self.lastCheckTimestamp = time.time()
        self.unresponsiveNodes = []
        self.mvipUp = True
        self.coreFiles = False
        self.xUnknownBlockID = False
        self.currentClusterFaults = set()
    
    def __dir__(self):
        return ["mvip", "lastCheckTimestamp", "unresponsiveNodes", "mvipUp", "coreFiles", "xUnknownBlockID", "currentClusterFaults"]
    
    def IsHealthy(self):
        if len(self.unresponsiveNodes) > 0: return False
        if not self.mvipUp: return False
        if self.xUnknownBlockID: return False
        if len(self.currentClusterFaults) > 0: return False
        if self.coreFiles: return False
        
        return True
    
    def IsSameHealthAs(self, otherClusterHealth):
        for item in dir(otherClusterHealth):
            if item == "lastCheckTimestamp": continue
            if getattr(self, item) != getattr(otherClusterHealth, item): return False
        return True
    
    def EmailSummary(self):
        summary = ""
        if not self.mvipUp:
            summary += "\nMVIP is not responding to ping"
        if len(self.unresponsiveNodes) > 0:
            summary += "\nNodes not responding to ping: " + ", ".join(self.unresponsiveNodes)
        if self.coreFiles:
            summary += "\nOne or more core files detected"
        if self.xUnknownBlockID:
            summary += "\nxUnknownBlockID detected"
        if len(self.currentClusterFaults) > 0:
            summary += "\nUnresolved cluster faults detected: " + ", ".join(self.currentClusterFaults)
        
        if summary == "":
            summary = "Cluster is healthy"
        
        return summary.lstrip()

def GetConfig():
    config_file = "sfhealthmon.json"
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
        required_keys = ["username", "password", "notify"]
        for key in required_keys:
            if not key in config_json["clusters"][cluster_mvip]:
                mylog.error("Missing required key '" + key + "' from cluster " + cluster_mvip + " in config file")
                error = True
        if not isinstance(config_json["clusters"][cluster_mvip]["notify"], list):
            mylog.error("notify key for cluster " + cluster_mvip + " must be a list")
            error = True
        if not isinstance(config_json["clusters"][cluster_mvip]["ignore_faults"], list):
            mylog.error("ignore_faults key for cluster " + cluster_mvip + " must be a list")
            error = True
        if error: continue

        cluster = ClusterToMonitor()
        cluster.mvip = cluster_mvip
        cluster.username = config_json["clusters"][cluster_mvip]["username"]
        cluster.password = config_json["clusters"][cluster_mvip]["password"]
        cluster.notify = config_json["clusters"][cluster_mvip]["notify"]
        cluster.renotify = config_json["clusters"][cluster_mvip]["renotify"]
        cluster.ignore_faults = set(config_json["clusters"][cluster_mvip]["ignore_faults"])
        clusters_from_config[cluster_mvip] = cluster
            
    if error: sys.exit(1)
    
    return config_json["monitor_interval"], clusters_from_config

def main():
    monitor_interval = 60
    clusters_to_monitor = dict()
    last_healthcheck = dict()
    current_healthcheck = dict()
    last_notification = dict()
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
            mylog.info("  Getting a list of nodes in the cluster")
            node_ips = []
            obj = libsf.CallApiMethod(cluster.mvip, cluster.username, cluster.password, "ListActiveNodes", {}, ExitOnError=False)
            for node in obj["nodes"]:
                node_ips.append(node["mip"])
            node_ips.sort()
            
            # Make sure all nodes are up
            mylog.info("  Checking that all nodes are up")
            for node_ip in node_ips:
                if not libsf.Ping(node_ip):
                    mylog.error("  Node " + node_ip + " is not responding to ping")
                    current_health.unresponsiveNodes.append(node_ip)
            
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
            
            # Look for core files on each node
            mylog.info("  Checking for core files on each node")
            for node_ip in node_ips:
                if node_ip not in current_health.unresponsiveNodes:
                    core_count = libsf.CheckCoreFiles(node_ip, ssh_user, ssh_pass)
                    if core_count > 0:
                        mylog.error("  Found " + str(core_count) + " core files on node " + node_ip)
                        current_health.coreFiles = True
            
            current_healthcheck[cluster.mvip] = current_health


        # Send notification if the cluster is unhealthy and it's health state has changed since the previous check
        for mvip in clusters_to_monitor.keys():
            current_health = current_healthcheck[mvip]
            notify = False
            
            # If this is the first time and it is unhealthy, send a notification
            if mvip not in last_healthcheck and not current_health.IsHealthy():
                notify = True
            
            elif mvip in last_healthcheck:
                previous_health = last_healthcheck[mvip]
                
            # If the health state has changed, send a notification
                if not current_health.IsSameHealthAs(previous_health):
                    mylog.debug("Sending notification because health state has changed")
                    notify = True
            # If the health state has not changed, but it is unhealthy and it has been at least the renotify interval since the last notification, send a notification
                if mvip in last_notification and not current_health.IsHealthy() and time.time() - last_notification[mvip] > 60 * cluster.renotify:
                    mylog.debug("Sending notification because renotify interval has expired")
                    notify = True
        
        if notify:
            mylog.info("  Sending notification to " + ", ".join(cluster.notify))
            last_notification[cluster.mvip] = time.time()
            libsf.SendEmail(cluster.notify, "Cluster " + cluster.mvip + " health report", current_health.EmailSummary(), pEmailFrom="healthmon@nothing")

        for mvip in clusters_to_monitor.keys():
            last_healthcheck[mvip] = current_healthcheck[mvip]

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
