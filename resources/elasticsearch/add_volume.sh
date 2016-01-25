#!/usr/bin/env bash
#MOUNT (AND FORMAT)
sudo mkfs -t ext4 /dev/xvdf

sudo mkdir /data4

# ADD TO /etc/fstab SO AROUND AFTER REBOOT
sudo sed -i '$ a\/dev/xvdf   /data4       ext4    defaults,nofail  0   2' /etc/fstab

# TEST IT IS WORKING
sudo mount -a
