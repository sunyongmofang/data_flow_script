#!/usr/bin/env python

from operator import add
import numpy as np

class AccidStat:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        clickid_sid_rdd = self.get_bs_rdd().filter(lambda line:line[10]=='3' and line[12]=='0' and line[13]=='0').map(lambda line:line[14])
        self.bp_click = self.get_bp_rdd().map(lambda line:line[5]).intersection(clickid_sid_rdd).count()

        fs_qualifying_rdd = self.get_fs_rdd().filter(lambda line:line[23]=='0' or line[23]=='1').map(lambda line:[ line[2], line[3], line[17], line[27] ])
        fs_qualifying_rdd.cache()

        self.huid_num = fs_qualifying_rdd.map(lambda line:(line[2][:3] if len(line[2])==9 else line[1]) +"\3"+line[3]).distinct().map(lambda line:line.split("\3")[0]).countByValue()
        self.ip_num = fs_qualifying_rdd.map(lambda line:(line[2][:3] if len(line[2])==9 else line[1]) +"\3"+line[0]).distinct().map(lambda line:line.split("\3")[0]).countByValue()
        self.ip_total = fs_qualifying_rdd.map(lambda line:line[0]).distinct().count()

        fs_qualifying_rdd.unpersist()

    def stop(self):
        param = self.platform.param
        lv_date = self.platform.lv_date
        biz_stat_all_jk_path = param["result"] + "biz_stat_all_jk"

        huid_num, ip_num, ip_total, bp_click = self.huid_num, self.ip_num, self.ip_total, self.bp_click
        accid_jk, bdname = {}, {}

        biz_stat_all_jk_file = open(biz_stat_all_jk_path, "r")
        data = biz_stat_all_jk_file.read()
        biz_stat_all_jk_file.close()

        #biz_stat_jk to biz_stat_accid
        save_list=[0, 2, 27, 8, 9, 0, 11, 0, 13, 0, 0, 21, 0, 0, 22, 26, 24, 25, 7, 0, 14, 7, 35, 34, 36, 37, 38]

        tmp_list = data.split("\n")
        tmp_list = [x.split("\t") for x in tmp_list]
        tmp_list = [x for x in tmp_list if len(x)>=39]
        for x in tmp_list:
            bdname[x[1][:3]] = x[31]
        tmp_list = [[x[i] if i!=0 else 0 for i in save_list] for x in tmp_list]
        total_list = [x for x in tmp_list if x[1]=="Total"][0]
        tmp_list = [x for x in tmp_list if x[1]!="Total"]
        tmp_list = [ [x[i] if i!=1 else x[i][:3] for i in xrange(len(x))] for x in tmp_list]
        tmp_list = [[str(i) for i in x] for x in tmp_list]
        tmp_list = [[int(i) if i.isdigit() else i for i in x] for x in tmp_list]
        tmp_list = [np.array(x) for x in tmp_list]

        accid_list = [str(x[1]) for x in tmp_list]
        accid_list = set(accid_list)
        accid_list = list(accid_list)

        for accid in accid_list:
            np_list = []
            for x in tmp_list:
                if str(x[1]) == str(accid):
                    np_list.append(x)
            if len(np_list) <= 1:
                accid_jk[accid] = np_list[0]
                continue
            accid_jk[accid] = sum(np_list)

        biz_stat_all_accid_file = open(param["result"]+"biz_stat_all_accid", "w")

        for accid in accid_jk:
            result_list = list(accid_jk[accid])
            result_list[1] = accid
            result_list[0] = lv_date
            result_list[9] = ip_num.get(accid, 0)
            result_list[10] = huid_num.get(accid, 0)
            result_list[19] = bdname.get(accid, 0)
            result_list = [str(x) for x in result_list]
            biz_stat_all_accid_file.write("\t".join(result_list)+"\n")

        total_list[0] = lv_date
        total_list[9] = ip_total
        total_list[10] = sum([huid_num[x] for x in huid_num])
        total_list[19] = ""
        total_list = [str(x) for x in total_list]
        biz_stat_all_accid_file.write("\t".join(total_list)+"\n")

        biz_stat_all_accid_file.close()

        accid_new = [0 for x in xrange(15)]
        accid_new[0] = lv_date
        accid_new[5] = total_list[4]
        accid_new[7] = bp_click

        biz_stat_all_new_file = open(param["result"]+"biz_stat_all_new", "w")
        accid_new = [str(x) for x in accid_new]
        biz_stat_all_new_file.write("\t".join(accid_new))
        biz_stat_all_new_file.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd


def getPluginClass():
    return AccidStat
