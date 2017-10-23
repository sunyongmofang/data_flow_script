#!/use/bin/env python

from operator import add
import numpy as np

class MlSidStat:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        bill_filter_rdd = self.get_bs_rdd()
        sid_ml_rdd = bill_filter_rdd.map(lambda line:(line[55],line[34])).filter(lambda (sid,ml):ml.strip("-").isdigit())

        sid_ml_deal_rdd1 = sid_ml_rdd.map(lambda (sid,ml):(sid,"9" if len(ml) == 1 and ml not in ["0","1"] else ml))
        sid_ml_deal_rdd2 = sid_ml_deal_rdd1.map(lambda (sid,ml):(sid,"100" if len(ml) >= 3 else ml))
        sid_ml_deal_rdd3 = sid_ml_deal_rdd2.map(lambda (sid,ml):(sid,ml[0]+"0" if len(ml) == 2 and ml != "-1" else ml))

        nullml = sid_ml_deal_rdd3.map(lambda (sid,ml):sid+"\3"+ml).countByValue()

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

        col = ["-1", "0", "1", "9", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]

        ml_distribution = open(param["result"] + "ml_distribution","w")
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
            ml_distribution.write("\t".join(tmp_list)+"\n")
        ml_distribution.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return MlSidStat
