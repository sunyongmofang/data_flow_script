#!/usr/bin/env python

from operator import add
import numpy as np

class PvSec:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        sc = self.platform.sc
        b = sc.broadcast(xrange(60))

        self.m = self.get_fs_rdd().filter(lambda line:line[0].isdigit()).map(lambda line:(line[17], line[0])).mapValues(int).mapValues(lambda t:t % 60)\
                    .mapValues(lambda t:[0 if t!=x else 1 for x in b.value]).mapValues(np.array).reduceByKey(add).mapValues(list).collectAsMap()

    def stop(self):
        param = self.platform.param
        m = self.m
        lv_date = self.platform.lv_date

        test_file = open(param["result"]+"pv_per_second", "w")
        for sid in m:
            if sum(m[sid]) < 1000:
                continue
            tmp_list = [lv_date, sid]
            tmp_list += m[sid]
            tmp_list.append(sum(m[sid]))
            tmp_list = [str(x) for x in tmp_list]
            test_file.write("\t".join(tmp_list) + "\n")
        test_file.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return PvSec
