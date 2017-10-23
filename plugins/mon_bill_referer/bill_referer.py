#!/usr/bin/env python

class FilterDaily:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        bill_result_zero_rdd = self.get_bs_rdd().filter(lambda line:line[12] == '0' and line[10] == "3").map(lambda line:[ line[3], line[16], line[55] ])
        bill_result_zero_rdd.cache()

        self.sid_h = bill_result_zero_rdd.map(lambda line:line[2]).countByValue()
        self.pvid_null = bill_result_zero_rdd.filter(lambda line:"pvid" not in line[1]).map(lambda line:line[2]).countByValue()
        self.pvid_diff = bill_result_zero_rdd.filter(lambda line:"pvid" in line[1] and line[0] not in line[1]).map(lambda line:line[2]).countByValue()

        bill_result_zero_rdd.unpersist()

    def stop(self):
        param = self.platform.param
        sid_h = self.sid_h
        pvid_null = self.pvid_null
        pvid_diff = self.pvid_diff
        lv_date = self.platform.lv_date

        sid_h_total = sum([sid_h[r] for r in sid_h])
        pvid_null_total = sum([pvid_null[r] for r in pvid_null])
        pvid_diff_total = sum([pvid_diff[r] for r in pvid_diff])

        mon_bill_referer_file = open(param["result"]+"mon_bill_referer", "w")
        for sid in sid_h:
            tmp_list = [lv_date, sid, sid_h.get(sid, 0), pvid_null.get(sid, 0), pvid_diff.get(sid, 0)]
            tmp_list = [str(x) for x in tmp_list]
            mon_bill_referer_file.write("\t".join(tmp_list)+"\n")
        tmp_list = [lv_date, "Total", sid_h_total, pvid_null_total, pvid_diff_total]
        tmp_list = [str(x) for x in tmp_list]
        mon_bill_referer_file.write("\t".join(tmp_list))
        mon_bill_referer_file.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return FilterDaily
