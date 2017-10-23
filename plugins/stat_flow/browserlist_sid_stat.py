#!/usr/bin/env python

from operator import add
import numpy as np

class BrowserList:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        bill_filter_rdd = self.get_bs_rdd()
        sid_ma_rdd = bill_filter_rdd.map(lambda line:(line[55],line[57])).filter(lambda (sid,ma):ma != "" and len(ma.split(",")) == 8)

        sid_ma_deal_rdd1 = sid_ma_rdd.map(lambda (sid,ma):(sid,ma.split(","))).map(lambda (sid,ma):(sid,ma[1])).filter(lambda (sid,historylen):(sid,historylen.strip("-").isdigit()))
        sid_ma_deal_rdd2 = sid_ma_deal_rdd1.map(lambda (sid,historylen):(sid,int(historylen))).map(lambda (sid,historylen):(sid,10 if historylen > 10 else historylen))

        sid_historylen = sid_ma_deal_rdd2.map(lambda (sid,historylen):sid+"\3"+str(historylen)).countByValue()

        self.sid_historylen = sid_historylen

    def stop(self):
        param = self.platform.param
        sid_historylen = self.sid_historylen

        lv_date = self.platform.lv_date

        sid_list = []
        for sid_ml in sid_historylen:
            sid = sid_ml.split("\3")[0]
            sid_list.append(sid)

        sid_set = set(sid_list)

        col = [str(x) for x in xrange(0,11)]

        browser_list_distribution_distribution = open(param["result"] + "browser_list_distribution","w")
        for sid in sid_set:
            tmp_list = [lv_date,sid]
            total = []
            for c in col:
                key = sid+"\3"+c
                v = sid_historylen.get(key,0)
                tmp_list.append(v)
                total.append(v)
            tmp_list.append(sum(total))
            tmp_list = [str(x) for x in tmp_list]
            if sum(total) > 100:
                browser_list_distribution_distribution.write("\t".join(tmp_list)+"\n")
        browser_list_distribution_distribution.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return BrowserList
