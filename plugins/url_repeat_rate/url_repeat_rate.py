#!/usr/bin/env python

from operator import add

class UrlRepeatRate:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        cache_fs_rdd = self.get_fs_rdd().filter(lambda line:line[23]=='0' or line[23]=='1').map(lambda line:[ line[7], line[17][:3] ])
        cache_fs_rdd.cache()

        self.r_pv = cache_fs_rdd.map(lambda line:line[1]).countByValue()
        self.accid_domain_map = cache_fs_rdd.filter(lambda line:len(line[0].split("/"))>=3 ).map(lambda line:(line[1], line[0])).mapValues(lambda v:v.split("/")[2]).map(lambda (k,v):(k+"\t"+v, 1)).reduceByKey(add).filter(lambda (k,v):v>10000).collectAsMap()

        cache_fs_rdd.unpersist()

    def stop(self):
        param = self.platform.param
        lv_date = self.platform.lv_date
        accid_domain_map = self.accid_domain_map
        r_pv = self.r_pv

        url_repeat_collect_file = open(param["result"]+"url_repeat_collect","w")
        for accid_domain in accid_domain_map:
            accid = accid_domain.split("\t")[0]
            total = r_pv.get(accid,0)
            num = accid_domain_map[accid_domain]
            if total==0 or float(num)/float(total)*100 < 5:
                continue
            tmp_list = [lv_date,accid_domain,num,total]
            tmp_list = [str(x) for x in tmp_list]
            result = "\t".join(tmp_list) + "\n"
            url_repeat_collect_file.write(result)
        url_repeat_collect_file.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

def getPluginClass():
    return UrlRepeatRate
