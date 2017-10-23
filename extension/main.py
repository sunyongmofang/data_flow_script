#!/usr/bin/env python

import struct
from socket import inet_aton

f = open("mydata4vipday2.dat", "rb")
binary = f.read()
f.close()
offset, = struct.unpack(">L", binary[:4])
index = binary[4:offset]

def find(ip, index, offset, binary):
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

print find("104.238.150.243", index, offset, binary)
tmp_list = find("104.238.150.243", index, offset, binary)
for t in tmp_list:
    print t,
