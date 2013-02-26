import os
import sys
# little hack to get to libs in the parent directory
sys.path.insert(0, "..")
import json
import time
import re
import ssh
import multiprocessing
import libsf
from libsf import mylog

ssh_user = "root"          # The username for the nodes
ssh_pass = "password"    # The password for the nodes if SSH keys are not set up

# Function to be run as a worker thread
def BundleThread(timestamp, folder, node_ip, node_user, node_pass):
    try:
        mylog.info(node_ip + ": Connecting")
        ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "hostname")
        hostname = stdout.readlines()[0].strip()

        # Create a support bundle
        mylog.info(node_ip + ": Generating support bundle for " + hostname)
        bundle_name = "bundle_" + timestamp
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "/sf/scripts/sf_make_support_bundle " + bundle_name + ";echo $?")
        data = stdout.readlines()
        retcode = int(data.pop())
        if retcode != 0:
            mylog.error(str(stderr.readlines()))
            ssh.close()
            return False
        
        bundle_name = bundle_name + "." + hostname + ".tar"

        # Compress the bundle using parallel gzip
        mylog.info(node_ip + ": Compressing bundle")
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "pigz " + bundle_name + ";echo $?")
        data = stdout.readlines()
        retcode = int(data.pop())
        if retcode != 0:
            mylog.error(str(stderr.readlines()))
            ssh.close()
            return False
        bundle_name = bundle_name + ".gz"

        # Copy the file to the local system
        mylog.info(node_ip + ": Saving bundle to " + folder + "/" + bundle_name)
        sftp = ssh.open_sftp()
        sftp.get(bundle_name, folder + "/" + bundle_name)
        sftp.close()

        # Remove the copy on the node
        libsf.ExecSshCommand(ssh, "rm " + bundle_name)
        ssh.close()
        mylog.info(node_ip + ": Finished")
        return True
    except Exception as e:
        mylog.error(str(e))
        return False


def main():
    node_list = []
    wait_time = 2
    folder = ""
    purge_threshold = 5
    while True:
        # Read configuration file
        config_file = "sfbundler.json"
        mylog.info("Reading config file " + config_file)
        config_handle = open(config_file, "r")

        # Remove comments from JSON before loading it
        config_lines = config_handle.readlines();
        new_config_text = ""
        for line in config_lines:
            line = re.sub("(//.+)", "", line)
            new_config_text += line
        config_json = json.loads(new_config_text)

        # Make sure all required params are present
        required_keys = ["node_ips", "wait_time", "folder", "purge_threshold"]
        error = False
        for key in required_keys:
            if not key in config_json.keys():
                mylog.error("Missing required key '" + key + "' from config file")
                error = True
        if error: exit(1)

        # Read config and update/log any that are different
        if node_list != config_json["node_ips"]:
            node_list = config_json["node_ips"]
            node_list.sort()
            mylog.info("New node list = " + ",".join(node_list))

        if wait_time != config_json["wait_time"]:
            wait_time = config_json["wait_time"]
            mylog.info("New wait time = " + str(wait_time) + " hours")

        if folder != config_json["folder"]:
            folder = config_json["folder"]
            mylog.info("New output folder = " + folder)

        if purge_threshold != config_json["purge_threshold"]:
            purge_threshold = config_json["purge_threshold"]
            mylog.info("New purge threshold = " + str(purge_threshold) + " days")

        # Create the output directory if it doesn't exist
        if (not os.path.exists(folder)):
            os.makedirs(folder)
        
        # Remove any bundles older than the threshold
        libsf.RunCommand("find " + folder + " -type f -mtime +" + str(purge_threshold) + " -name \"bundle*\" | xargs rm")
    
        # Start one thread per node
        threads = []
        timestamp = time.strftime("%Y-%m-%d-%H-%M-%S")
        for node_ip in node_list:
            th = multiprocessing.Process(target=BundleThread, args=(timestamp, folder, node_ip, ssh_user, ssh_pass))
            th.start()
            threads.append(th)
    
        # Wait for all threads to finish
        for th in threads:
            th.join()
                
        # Wait for wait_time hours
        mylog.info("SFBundler waiting for " + str(wait_time) + " hours before getting next support bundle")
        time.sleep(wait_time * 60 * 60)

if __name__ == '__main__':
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
