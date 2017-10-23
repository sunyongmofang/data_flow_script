#!/usr/bin/env python

from operator import add
import numpy as np

class SidStat:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        bill_filter_rdd = self.get_bs_rdd().filter(lambda line:line[12]=='0' and line[13]=='0')
        clk = bill_filter_rdd.map(lambda line:line[55]).countByValue()
        nullml = bill_filter_rdd.filter(lambda line:line[34]=='-1').map(lambda line:line[55]).countByValue()
        nullmc = bill_filter_rdd.filter(lambda line:line[35]=='-1').map(lambda line:line[55]).countByValue()
        time_5 = lambda line:line[0].isdigit() and line[6].isdigit() and ( ( int(line[0]) - int(line[6]) ) < 5 )
        time_10 = lambda line:line[0].isdigit() and line[6].isdigit() and ( ( int(line[0]) - int(line[6]) ) < 10 )
        pvclk5 = bill_filter_rdd.filter(time_5).map(lambda line:line[55]).countByValue()
        pvclk10 = bill_filter_rdd.filter(time_10).map(lambda line:line[55]).countByValue()

        self.clk = clk
        self.nullml = nullml
        self.nullmc = nullmc
        self.pvclk5 = pvclk5
        self.pvclk10 = pvclk10

    def stop(self):
        param = self.platform.param
        clk = self.clk
        nullml = self.nullml
        nullmc = self.nullmc
        pvclk5 = self.pvclk5
        pvclk10 = self.pvclk10

        lv_date = self.platform.lv_date

        anticheat_sid_stat_file = open(param["result"] + "anticheat_sid_stat","w")
        for sid in clk:
            tmp_list = [lv_date,sid,clk[sid],nullml.get(sid,0),nullmc.get(sid,0),pvclk5.get(sid,0),pvclk10.get(sid,0)]
            tmp_list = [str(x) for x in tmp_list]
            result_str = "\t".join(tmp_list) + "\n"
            anticheat_sid_stat_file.write(result_str)
        anticheat_sid_stat_file.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return SidStat
