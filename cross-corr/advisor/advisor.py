import os
import pyhdfs
from pprint import pprint

client = pyhdfs.HdfsClient(hosts="cross-corr-namenode-1:9870", user_name="hadoop")

item = input("Enter item name (item[0-999]): ")

if not (item.startswith("item") and item[4:].isdigit() and 0 <= int(item[4:]) <= 999):
    print("Invalid input")
    os._exit(1)

ccr_files = client.listdir("/ccr/output")
ccr_data = []
for f in ccr_files:
    if f.startswith("part-"):
        data = client.open("/ccr/output/" + f).read().decode()
        ccr_data.extend(sorted(
            (line.split() for line in data.splitlines()
            if item in line.split()),
            reverse=True, key=lambda x: int(x[2])
        )[:10])
ccr_data = sorted(ccr_data, key=lambda x: int(x[2]), reverse=True)
print("CCR top 10:")
pprint(ccr_data)


ccs_files = client.listdir("/ccs/output")
ccs_data = []
for f in ccs_files:
    if f.startswith("part-"):
        data = client.open("/ccs/output/" + f).read().decode()
        ccs_data.extend(sorted(
            (line.split() for line in data.splitlines()
            if item in line.split()),
            reverse=True, key=lambda x: int(x[2])
        )[:10])
ccs_data = sorted(ccs_data, key=lambda x: int(x[2]), reverse=True)
print("CCS top 10:")
pprint(ccs_data)
