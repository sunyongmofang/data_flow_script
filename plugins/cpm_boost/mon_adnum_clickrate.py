#!/usr/bin/env python

from operator import add
import numpy as np

class AdNumClickRate:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        fs_pvid_k = self.get_fs_rdd().filter(lambda line:line[23]=='0' or line[23]=='1')\
                        .map(lambda line: ( line[10], [ line[15], line[20], '1' if line[21]!='' else '0', line[25], line[36], line[38] ] )).mapValues(lambda v:"\3".join(v)).filter(lambda (k,v):len(v)<15)
        bs_pvid_k = self.get_bs_rdd().filter(lambda line:line[12]=='0' and line[13]=='0').map(lambda line: ( line[3], '1' ))

        fs_pvid_k.cache()

        self.fs_result = fs_pvid_k.map(lambda (k,v):v).countByValue()
        self.bs_result = bs_pvid_k.leftOuterJoin(fs_pvid_k).mapValues(lambda v:v[1]).map(lambda (k,v):v).filter(lambda v:v!=None).countByValue()

        fs_pvid_k.unpersist()

    def stop(self):
        fs_result, bs_result = self.fs_result, self.bs_result
        lv_date = self.platform.lv_date
        param = self.platform.param

        fs_result_map = self.result_count(fs_result)
        bs_result_map = self.result_count(bs_result)

        i = 1

        mon_adnum_clickrate_file = open(param['result'] + "mon_adnum_clickrate", "w")
        for k in fs_result_map:
            if fs_result_map[k]==0 and bs_result_map[k]==0:
                continue
            tmp_list = [lv_date, i, k, fs_result_map[k], bs_result_map[k], fs_result_map['Total']]
            tmp_list = [str(x) for x in tmp_list]
            mon_adnum_clickrate_file.write("\t".join(tmp_list)+"\n")
            i += 1

        mon_adnum_clickrate_file.close()

    def result_count(self, result):
        result_map = {"Total":0, "Pic Commonweal":0, "Pic Second Commonweal":0, "Pic Non-commonweal":0, "Txt Second Non-commonweal":0, "No Ads":0, \
                    "No Request Baidu":0, "Xml Error":0, "Pic Second Non-commonweal":0, "Txt Second Commonweal":0, "Txt Commonweal":0, "Txt Non-commonweal":0, "Other":0}
        for k in result:
            tmp_list = k.split("\3")
            v = result[k]

            result_map["Total"] += result[k]

            if tmp_list[5] == '0':
                result_map["No Request Baidu"] += v
            elif tmp_list[0] == '-1':
                result_map["Xml Error"] += v
            elif tmp_list[2] == '0':
                result_map["No Ads"] += v
            elif tmp_list[1] == '2' and tmp_list[3] == '0' and tmp_list[4] == '1':
                result_map["Pic Non-commonweal"] += v
            elif tmp_list[1] == '2' and tmp_list[3] != '0' and tmp_list[4] == '1':
                result_map["Pic Commonweal"] += v
            elif tmp_list[1] == '2' and tmp_list[3] == '0' and tmp_list[4] == '2':
                result_map["Pic Second Non-commonweal"] += v
            elif tmp_list[1] == '2' and tmp_list[3] != '0' and tmp_list[4] == '2':
                result_map["Pic Second Commonweal"] += v
            elif tmp_list[1] == '1' and tmp_list[3] == '0' and tmp_list[4] == '1':
                result_map["Txt Non-commonweal"] += v
            elif tmp_list[1] == '1' and tmp_list[3] != '0' and tmp_list[4] == '1':
                result_map["Txt Commonweal"] += v
            elif tmp_list[1] == '1' and tmp_list[3] == '0' and tmp_list[4] == '2':
                result_map["Txt Second Non-commonweal"] += v
            elif tmp_list[1] == '1' and tmp_list[3] != '0' and tmp_list[4] == '2':
                result_map["Txt Second Commonweal"] += v
            else:
                result_map["Other"] += v

        return result_map

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return AdNumClickRate
