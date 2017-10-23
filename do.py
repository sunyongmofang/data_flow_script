#!/usr/bin/python

import sys, struct
from core import CoreClazz
from socket import inet_aton

replaceMap = {}
param = {}
split_str = "\3"

binary = open("extension/mydata4vipday2.dat", "rb").read()
offset, = struct.unpack(">L", binary[:4])
index = binary[4:offset]

#find ip change region
def find(ip):
    if ip == "":
        return "N/A"
    nip = inet_aton(ip)
    ipdot = ip.split('.')
    if int(ipdot[0]) < 0 or int(ipdot[0]) > 255 or len(ipdot) != 4:
        return "N/A"

    tmp_offset = int(ipdot[0]) * 4
    start, = struct.unpack("<L", index[tmp_offset:tmp_offset + 4])

    index_offset = index_length = 0
    max_comp_len = offset - 1028
    start = start * 8 + 1024
    while start < max_comp_len:
        if index[start:start + 4] >= nip:
            index_offset, = struct.unpack("<L", index[start + 4:start + 7] + chr(0).encode('utf-8'))
            index_length, = struct.unpack("B", index[start + 7])
            break
        start += 8

    if index_offset == 0:
        return "N/A"

    res_offset = offset + index_offset - 1024
    return binary[res_offset:res_offset + index_length].split("\t")

#load configure file
def loadParam():
    f = open("ini/param.ini")
    confstr = f.read()
    confarray = confstr.replace("\\\n","").replace("\t","").replace(" ","").split("\n")
    for line in confarray:
        if "=" not in line or "#" == line[0]:
            continue
        ls = line.split("=")
        k = ls[0]
        v = ls[1]
        for key, value in replaceMap.items():
            v = v.replace(key, value)
        if "[" in v and "]" in v:
            v = v.strip("[").strip("]").split(",")
            v = [(int(ele) if ele.isdigit() else ele) for ele in v]
        param[k] = v

def main():
    lv_date, log_date, root_path, hadoopsw = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    replaceMap["{lv_date}"] = lv_date
    replaceMap["{log_date}"] = log_date
    replaceMap["{hadoop_log_date}"] = "-".join([log_date[0:4],log_date[4:6],log_date[6:8]])
    replaceMap["{root_path}"] = root_path
    replaceMap["{hadoopsw}"] = hadoopsw
    loadParam()
    co = CoreClazz(lv_date, log_date, param, split_str)
    co.ip_region = find
    co.startover()
    co.shutdown()

if __name__ == "__main__":
    main()
