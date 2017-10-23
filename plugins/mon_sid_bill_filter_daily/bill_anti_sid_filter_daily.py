#!/usr/bin/env python

from operator import add
import numpy as np

class FilterDaily:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        cache_rdd = self.get_bs_rdd().map(lambda line:[ line[10], line[12], line[13], line[55] ]).filter(lambda line:line[1].replace("-","").isdigit() and line[2].replace("-","").isdigit())
        cache_rdd.cache()

        self.bill_s = cache_rdd.map(lambda line:line[3]).countByValue()
        self.bill_filter = cache_rdd.filter(lambda line:line[0]=="3").map(lambda line:(line[3], line[1])).mapValues(lambda v: [0 if x!=int(v) else 1 for x in xrange(12)]).mapValues(np.array).reduceByKey(add).collectAsMap()

        self.bill_flag = cache_rdd.map(lambda line:(line[3], line[0])).mapValues(lambda v:[1 if v==x else 0 for x in ["2", "3", "0"]]).mapValues(np.array).reduceByKey(add).collectAsMap()

        self.anti_s = cache_rdd.filter(lambda line:line[1]=="0").map(lambda line:line[3]).countByValue()
        self.anti_filter = cache_rdd.filter(lambda line:line[1]=="0" and line[0]=="3").map(lambda line:(line[3], line[2])).mapValues(lambda v:"1" if v=="0" else bin(int(v)).replace("0b","")+"0")\
                .mapValues(lambda v:"0"*(19-len(v))+v).mapValues(lambda v:[int(x) for x in v]).mapValues(np.array).reduceByKey(add).collectAsMap()

        self.anti_flag = cache_rdd.filter(lambda line:line[1]=="0").map(lambda line:(line[3], line[0])).mapValues(lambda v:[1 if v==x else 0 for x in ["2", "3", "0"]]).mapValues(lambda v:np.array(v)).reduceByKey(add).collectAsMap()

        cache_rdd.unpersist()

    def stop(self):
        bill_s = self.bill_s
        bill_filter = self.bill_filter
        bill_flag = self.bill_flag
        anti_s = self.anti_s
        anti_filter = self.anti_filter
        anti_flag = self.anti_flag

        bill_s_total = sum([x for y,x in bill_s.items()])
        bill_filter_total = sum([x for y,x in bill_filter.items()])
        bill_flag_total = sum([x for y,x in bill_flag.items()])

        anti_s_total = sum([x for y,x in anti_s.items()])
        anti_filter_total = list( sum([x for y,x in anti_filter.items()]) )
        anti_flag_total = sum([x for y,x in anti_flag.items()])

        anti_filter_total.reverse()

        param = self.platform.param
        lv_date = self.platform.lv_date

        mon_bill_sid_filter_daily_file = open(param["result"] + "mon_bill_sid_filter_daily", "w")

        for sid in bill_s:
            tmp_list = [lv_date, sid]
            tmp_list.append(bill_s.get(sid, 0))
            tmp_list += bill_filter.get(sid, [0 for x in xrange(12)])
            tmp_list += bill_flag.get(sid, [0 for x in xrange(3)])
            tmp_list = [str(x) for x in tmp_list]
            mon_bill_sid_filter_daily_file.write("\t".join(tmp_list) + "\n")

        tmp_list = [lv_date, "Total"]
        tmp_list.append(bill_s_total)
        tmp_list += bill_filter_total
        tmp_list += bill_flag_total
        tmp_list = [str(x) for x in tmp_list]
        mon_bill_sid_filter_daily_file.write("\t".join(tmp_list) + "\n")

        mon_bill_sid_filter_daily_file.close()

        tmp_list.remove("Total")
        mon_bill_filter_daily_file = open(param["result"] + "mon_bill_filter_daily", "w")
        mon_bill_filter_daily_file.write("\t".join(tmp_list))
        mon_bill_filter_daily_file.close()

        mon_anti_sid_filter_daily_file = open(param["result"] + "mon_anti_sid_filter_daily", "w")

        for sid in anti_s:
            tmp_list = [lv_date, sid]
            tmp_list.append(anti_s.get(sid, 0))
            anti_filter_list = list( anti_filter.get(sid, [0 for x in xrange(19)]) )
            anti_filter_list.reverse()
            tmp_list += anti_filter_list
            tmp_list += anti_flag.get(sid, [0 for x in xrange(3)])
            tmp_list = [str(x) for x in tmp_list]
            mon_anti_sid_filter_daily_file.write("\t".join(tmp_list) + "\n")

        tmp_list = [lv_date, "Total"]
        tmp_list.append(anti_s_total)
        tmp_list += anti_filter_total
        tmp_list += anti_flag_total
        tmp_list = [str(x) for x in tmp_list]
        mon_anti_sid_filter_daily_file.write("\t".join(tmp_list) + "\n")

        mon_anti_sid_filter_daily_file.close()

        tmp_list.remove("Total")
        mon_anti_filter_daily_file = open(param["result"] + "mon_anti_filter_daily", "w")
        mon_anti_filter_daily_file.write("\t".join(tmp_list))
        mon_anti_filter_daily_file.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return FilterDaily
