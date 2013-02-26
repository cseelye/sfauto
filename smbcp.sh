#!/bin/bash
if [ "$#" -lt "2" ]; then
    echo "Copy files to a Windows machine"
    echo "Usage `basename $0` localfile username%password@host:filepath\\remotefile"
    echo "Example: `basename $0` my_script.ps1 administrator%password@winproxy:/scripts"
    exit 1
fi

localfile=$1
IFS='@' read -a PIECES <<< "$2"
IFS='%' read -a CREDS <<< "${PIECES[0]}"
username=${CREDS[0]}
password=${CREDS[0]}
IFS=':' read -a PIECES2 <<< "${PIECES[1]}"
server=${PIECES2[0]}
remote_path=${PIECES[1]}

smbclient //$server/c\$ $password -U $username << EOC
cd $remote_path
put $1
exit
EOC
