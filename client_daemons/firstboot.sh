#!/bin/bash

# To set this up on a template, put this file into /opt/firstboot/firstboot.sh, put firstboot.conf into /etc/init and run initctl reload-configuration
# rm /opt/firstboot/firstbootdone
# Shut down without rebooting
# Any time you boot the template after that, firstboot will run and create /opt/firstboot/firstbootdone, so make sure to remove it before shutting down and cloning

# Do nothing if we have already run
if [ -e /opt/sfauto/client_daemons/firstbootdone ]; then
    /usr/bin/logger -s -t firstboot "Exiting because firstboot has already run"
    exit
fi

# Detect the hypervisor we are running on
HYPERVISOR=$(/usr/sbin/virt-what | /usr/bin/head -1 | /usr/bin/awk '{ print tolower($0) }')

# Try to read our VM name from the hypervisor
if [[ "$HYPERVISOR" == *xen* ]]; then
    VM_NAME=$(/usr/bin/xenstore-read name 2>/dev/null)
fi  
if [[ "$HYPERVISOR" == *vmware* ]]; then
    VM_NAME=$(/usr/sbin/vmtoolsd --cmd "info-get guestinfo.hostname" 2>/dev/null)
fi
if [[ "$HYPERVISOR" == *hyperv* ]]; then
    VM_NAME=$(/bin/cat /var/opt/hyperv/.kvp_pool_3 | /bin/sed 's/\x0/ /g' | /usr/bin/awk '{print $20}')
fi

# Skip running firstboot if the VM name contains gold or template
if [[ -n "$VM_NAME" && ( "$VM_NAME" == *gold* || "$VM_NAME" == *template* ) ]]; then
    /usr/bin/logger -s -t firstboot "Skipping first boot setup because my VM name looks like template VM"
    exit
fi


# Alphabetically first MAC addr to use as unique ID
MAC=`/sbin/ifconfig -a | /bin/grep HWaddr | /usr/bin/awk '{print $5}' | /bin/sed 's/://g' | /usr/bin/sort | /usr/bin/head -1 | /usr/bin/awk '{ print tolower($0) }'`
#logger -s -t firstboot "My MAC is $MAC"

# If you know the MAC of your template, you can use it here to skip running on your template
# Note all letters in lower case!
if [[ "$MAC" == "005056ac49d1" ]];
then
    /usr/bin/logger -s -t firstboot "Skipping first boot setup because my MAC looks like a template VM"
    exit
fi

/usr/bin/logger -s -t firstboot "Running one time setup"

/usr/bin/logger -s -t firstboot "  Fixing /etc/hosts"
/usr/bin/chattr -i /etc/hosts
/bin/echo -e "127.0.0.1\tlocalhost\n" > /etc/hosts

/usr/bin/logger -s -t firstboot "  Clearing udev rules"
/bin/rm -f /etc/udev/rules.d/70-persistent-net.rules

# Ubuntu
if /bin/uname -a | /bin/grep -qi "ubuntu"; then
    /usr/bin/logger -s -t firstboot "  Detected Ubuntu"
    
    /usr/bin/logger -s -t firstboot "  Setting networking to DHCP"
    /bin/echo "# Created by firstboot script" > /etc/network/interfaces
    /bin/echo -e "auto lo\niface lo inet loopback\n" > /etc/network/interfaces
    for dev in $(/sbin/ifconfig -a | /bin/egrep "^\S" | /bin/grep -v lo | /usr/bin/awk '{print $1}' | /usr/bin/sort); do
        /usr/bin/logger -s -t firstboot "  Adding $dev to /etc/network/interfaces"
        /bin/echo -e "auto $dev\niface $dev inet dhcp\n" >> /etc/network/interfaces
    done
    
    HOSTNAME="ubuntu-$MAC"
    if [[ -n "$VM_NAME" ]]; then
        HOSTNAME=$VM_NAME
    fi
    /usr/bin/logger -s -t firstboot "  Setting hostname to $HOSTNAME"
    /bin/echo "$HOSTNAME" > /etc/hostname
fi

# RHEL
if /bin/uname -a | /bin/grep -qi "el"; then
    /usr/bin/logger -s -t firstboot "  Detected RHEL"
    /usr/bin/logger -s -t firstboot "  Setting networking to DHCP"
    
    for dev in $(/sbin/ifconfig -a | /bin/egrep "^\S" | /bin/grep -v lo | /usr/bin/awk '{print $1}' | /usr/bin/sort); do
        /usr/bin/logger -s -t firstboot "Creating ifcfg-$dev"
        /bin/rm -f /etc/sysconfig/network-scripts/ifcfg-$dev
        /bin/echo -e "DEVICE=$dev\nNM_CONTROLLED=no\nBOOTPROTO=dhcp\nONBOOT=yes" > /etc/sysconfig/network-scripts/ifcfg-$dev
    done

    /usr/bin/logger -s -t firstboot "  Setting hostname to rhel-$MAC"
    /bin/sed -e 's/HOSTNAME=.*/HOSTNAME=rhel-'$MAC'/' /etc/sysconfig/network
fi
rm -f /root/shutdown_template.sh

/usr/bin/logger -s -t firstboot "Rebooting..."
/bin/touch /opt/sfauto/client_daemons/firstbootdone
/sbin/reboot
