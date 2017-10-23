#!/usr/bin/env python

from operator import add
import numpy as np

class MCSidStat:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        bill_filter_rdd = self.get_bs_rdd()
        sid_ml_rdd = bill_filter_rdd.map(lambda line:(line[55],line[57])).filter(lambda (sid,ma):ma != "" and len(ma.split(",")) == 8).filter(lambda (sid,ma): False if False in [x.isdigit() for x in ma.split(",")] else True)

        sid_ml_deal_rdd1 = sid_ml_rdd.map(lambda (sid,ma):(sid,ma.split(","))).map(lambda (sid,ma):(sid,((float(ma[2])-float(ma[4]))*(float(ma[2])-float(ma[4]))+(float(ma[3])-float(ma[5]))*(float(ma[3])-float(ma[5])))**0.5))
        sid_ml_deal_rdd2 = sid_ml_deal_rdd1.map(lambda (sid,distance):(sid,int(distance))).map(lambda (sid,distance):(sid,100 if distance > 100 else distance))
        sid_ml_deal_rdd3 = sid_ml_deal_rdd2.map(lambda (sid,distance):(sid,distance/10*10))

        nullml = sid_ml_deal_rdd3.map(lambda (sid,distance):sid+"\3"+str(distance)).countByValue()

        self.nullml = nullml

    def stop(self):
        param = self.platform.param
        nullml = self.nullml

        lv_date = self.platform.lv_date

        sid_list = []
        for sid_ml in nullml:
            sid = sid_ml.split("\3")[0]
            sid_list.append(sid)

        sid_set = set(sid_list)

        col = ["0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]

        ml_distribution = open(param["result"] + "mouse_distance_distribution","w")
        for sid in sid_set:
            tmp_list = [lv_date,sid]
            total = []
            for c in col:
                key = sid+"\3"+c
                v = nullml.get(key,0)
                tmp_list.append(v)
                total.append(v)
            tmp_list.append(sum(total))
            tmp_list = [str(x) for x in tmp_list]
            if sum(total) > 100:
                ml_distribution.write("\t".join(tmp_list)+"\n")
        ml_distribution.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return MCSidStat
