#!/bin/bash
echo "========= Shutting down template VM =========" > /var/log/syslog
rm -f /opt/sfauto/client_daemons/firstbootdone
shutdown -h now
