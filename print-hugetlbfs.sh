#!/bin/bash
mkdir -p /mnt/huge2M
mkdir -p /mnt/huge1G
mount -t hugetlbfs -o pagesize=1G none /mnt/huge1G
mount -t hugetlbfs -o pagesize=2M none /mnt/huge2M

for i in {0..7}
do
	if [[ -e "/sys/devices/system/node/node$i" ]]
	then
		printf "Node %s\n" "$i"
		# 2 MiB Hugepages
		printf "2MiB Hugepages:\nTotal: %s\n" "$(cat /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages)"
		printf "Free: %s\n" "$(cat /sys/devices/system/node/node0/hugepages/hugepages-2048kB/free_hugepages)"

		# 1 GiB Hugepages
		printf "1GiB Hugepages:\nTotal: %s\n" "$(cat /sys/devices/system/node/node0/hugepages/hugepages-1048576kB/nr_hugepages)"
		printf "Free: %s\n\n" "$(cat /sys/devices/system/node/node0/hugepages/hugepages-1048576kB/free_hugepages)"
	fi
done