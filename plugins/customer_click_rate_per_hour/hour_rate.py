#!/usr/bin/env python

from __future__ import division
from operator import add
import numpy as np

class HourRate:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        hit_list = lambda v:[0 if v!=x else 1 for x in xrange(24)]
        get_hour = lambda t:((t%86400-t%86400%3600)/3600+8)%24

        cache_bs_rdd = self.get_bs_rdd().map(lambda line:[ line[0], line[55][:3] ]).filter(lambda line:line[0].isdigit() and line[1].isdigit())
        cache_fs_rdd = self.get_fs_rdd().filter(lambda line:line[23]=='0' or line[23]=='1').map(lambda line:[ line[0], line[17][:3] ]).filter(lambda line:line[0].isdigit() and line[1].isdigit())

        self.accid_hour_click = cache_bs_rdd.map(lambda line:(line[1], line[0])).mapValues(int).mapValues(get_hour).mapValues(int).mapValues(hit_list).mapValues(np.array).reduceByKey(add).filter(lambda (k,v):sum(v)>20000).collectAsMap()
        self.accid_hour_pv = cache_fs_rdd.map(lambda line:(line[1], line[0])).mapValues(int).mapValues(get_hour).mapValues(int).mapValues(hit_list).mapValues(np.array).reduceByKey(add).filter(lambda (k,v):sum(v)>2500).collectAsMap()

    def stop(self):
        param, lv_date = self.platform.param, self.platform.lv_date
        accid_hour_pv = self.accid_hour_pv
        accid_hour_click = self.accid_hour_click

        per_hour_click_rate_file = open(param["result"]+"per_hour_click_rate","w")
        for accid in accid_hour_click:
            tmp_list = [lv_date, accid]
            pv = [x if x!=0 else -1 for x in accid_hour_pv.get(accid, [0 for i in xrange(24)])]
            click_rate = accid_hour_click[accid] / np.array(pv) * np.array([100 for x in xrange(24)])
            click_rate = [x if x!=-0 else -1 for x in click_rate]
            tmp_list += list(click_rate)
            tmp_list = [str(x) for x in tmp_list]
            result_str = "\t".join(tmp_list) + "\n"
            per_hour_click_rate_file.write(result_str)
        per_hour_click_rate_file.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return HourRate
