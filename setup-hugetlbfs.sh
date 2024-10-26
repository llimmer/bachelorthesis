#!/bin/bash
mkdir -p /mnt/huge2M
mkdir -p /mnt/huge1G
mount -t hugetlbfs -o pagesize=1G none /mnt/huge1G
mount -t hugetlbfs -o pagesize=2M none /mnt/huge2M

for i in {0..7}
do
	if [[ -e "/sys/devices/system/node/node$i" ]]
	then
		echo 5000 > /sys/devices/system/node/node$i/hugepages/hugepages-2048kB/nr_hugepages
		echo 80 > /sys/devices/system/node/node$i/hugepages/hugepages-1048576kB/nr_hugepages
	fi
done