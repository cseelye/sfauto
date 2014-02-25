import json
import sys
sys.path.append("..")
import lib.libsf as libsf

# Read config info
with open("dogfood_config.json", "r") as f:
    config = json.load(f)

session_list = libsf.CallApiMethod(config["mvip"], config["username"], config["password"], "ListISCSISessions", {} )

clients = {}
for session in session_list["sessions"]:
    ip_port = session["initiatorIP"]
    client_ip = ip_port.split(":")[0]
    client_iqn = session["initiatorName"]
    client_name = client_iqn.split(":")[-1]
    if client_name not in clients.keys():
        clients[client_name] = client_ip

print
print "Connected Clients"
print
for client_name in sorted(clients.keys()):
    print "%12s  %s" % (client_name, clients[client_name])
