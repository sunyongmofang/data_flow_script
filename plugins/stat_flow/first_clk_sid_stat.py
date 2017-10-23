#!/usr/bin/env python

from operator import add
import numpy as np

class MCSidStat:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        bill_filter_rdd = self.get_bs_rdd().filter(lambda line:line[0].isdigit() and line[25].isdigit())

        sid_sec_rdd = bill_filter_rdd.map(lambda line:(line[55],int(line[0])-int(line[25])))

        step1 = sid_sec_rdd.map(lambda (sid,sec):(sid,0 if sec<300 else sec))
        step11 = step1.map(lambda (sid,sec):(sid,300 if sec>=300 else sec))
        nullmc = step11.map(lambda (sid,sec):sid+"\3"+str(sec)).countByValue()

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

        col = ["0","300"]

        mc_distribution = open(param["result"] + "first_clk_distribution","w")
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
    return MCSidStat
