#!/bin/bash
echo "========= Shutting down template VM =========" > /var/log/syslog
rm -f /opt/firstboot/firstbootdone
shutdown -h now
