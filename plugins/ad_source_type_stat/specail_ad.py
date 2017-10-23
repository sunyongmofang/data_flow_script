#!/usr/bin/env python

from operator import add
import numpy as np

class AdSource:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        ad_charge_list = ["900170201", "900180201", "900190201"]
        hit_num = lambda v:[1 if v==x else 0 for x in ad_charge_list]

        cache_bs_rdd = self.get_bs_rdd().map(lambda line:[ line[40], line[55] ])
        cache_bs_rdd.cache()
        self.click1 = cache_bs_rdd.map(lambda line:line[1]).countByValue()
        self.click2 = cache_bs_rdd.filter(lambda line:line[0] in ad_charge_list).map(lambda line:(line[1], line[0])).mapValues(hit_num).mapValues(np.array).reduceByKey(add).mapValues(list).collectAsMap()
        cache_bs_rdd.unpersist()

        cache_fs_rdd = self.get_fs_rdd().filter(lambda line:line[23]=='0' or line[23]=='1').map(lambda line:[ line[4], line[17] ])
        cache_fs_rdd.cache()
        self.pv1 = cache_fs_rdd.map(lambda line:line[1]).countByValue()
        self.pv2 = cache_fs_rdd.filter(lambda line:line[0] in ad_charge_list).map(lambda line:(line[1], line[0])).mapValues(hit_num).mapValues(np.array).reduceByKey(add).mapValues(list).collectAsMap()
        cache_fs_rdd.unpersist()

    def stop(self):
        param = self.platform.param
        lv_date = self.platform.lv_date
        pv1, click1, pv2, click2 = self.pv1, self.click1, self.pv2, self.click2
        ad_name_list = ["commweal_click_rate", "transparent_click_rate", "nonclick_click_rate"]

        sid_set = set([sid for sid in pv2] + [sid for sid in click2])

        for i in xrange(len(ad_name_list)):
            file_name = ad_name_list[i]
            fp = open(param["result"] + file_name, "w")
            for sid in sid_set:
                if pv2.get(sid, [0,0,0])[i] == 0 and click2.get(sid, [0,0,0])[i] == 0:
                    continue
                tmp_list = [lv_date, sid, pv1.get(sid, 0), click1.get(sid ,0), pv2.get(sid, [0,0,0])[i], click2.get(sid, [0,0,0])[i]]
                tmp_list = [str(x) for x in tmp_list]
                fp.write("\t".join(tmp_list) + "\n")
            fp.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return AdSource
