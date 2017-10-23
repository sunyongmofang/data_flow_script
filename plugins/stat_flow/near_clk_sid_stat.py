#!/usr/bin/env python

from operator import add
import numpy as np

class NCLKSidStat:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        bill_filter_rdd = self.get_bs_rdd().filter(lambda line:line[0].isdigit() and line[27].isdigit())

        sid_sec_rdd = bill_filter_rdd.map(lambda line:(line[55],int(line[0])-int(line[27])))
        nullmc = sid_sec_rdd.map(lambda (sid,sec):(sid,100 if sec>=100 and sec<600 else sec)).map(lambda (sid,sec):(sid,600 if sec>=600 else sec)).map(lambda (sid,sec):(sid,sec/10*10)).map(lambda (sid,sec):sid+"\3"+str(sec)).countByValue()

        self.nullmc = nullmc

    def stop(self):
        param = self.platform.param
        nullmc = self.nullmc

        lv_date = self.platform.lv_date

        sid_list = []
        for sid_mc in nullmc:
            sid = sid_mc.split("\3")[0]
            sid_list.append(sid)

        sid_set = set(sid_list)

        col = ["0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100", "600"]

        mc_distribution = open(param["result"] + "near_clk_distribution","w")
        for sid in sid_set:
            tmp_list = [lv_date,sid]
            total = []
            for c in col:
                key = sid+"\3"+c
                v = nullmc.get(key,0)
                tmp_list.append(v)
                total.append(v)
            tmp_list.append(sum(total))
            tmp_list = [str(x) for x in tmp_list]
            mc_distribution.write("\t".join(tmp_list)+"\n")
        mc_distribution.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return NCLKSidStat
