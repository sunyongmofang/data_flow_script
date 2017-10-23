#!/usr/bin/env python

from operator import add

class BdTxtCount:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        fs_qualifying_rdd = self.get_fs_rdd().filter(lambda line:line[20]=="1" and line[38]=="1" and line[25]=="0" and line[15]=="0" and line[0].isdigit()).map(lambda line:[ line[0], line[21] ])
        fs_qualifying_rdd.cache()
        self.total = fs_qualifying_rdd.map(lambda line:line[1]).distinct().count()
        self.hours = fs_qualifying_rdd.map(lambda line:( (((int(line[0])%86400)/3600)+8)%24 ,line[1])).distinct().countByKey()
        fs_qualifying_rdd.unpersist()

    def stop(self):
        param = self.platform.param
        lv_date = self.platform.lv_date
        total = self.total
        hours = self.hours

        picture_url_count_file = open(param["result"]+"text_url_count","w")
        for x in xrange(24):
            tmp_list = []
            tmp_list.append(lv_date)
            tmp_list.append("%02d" % x)
            tmp_list.append(hours.get(x,0))
            tmp_list.append(total)
            tmp_list = [str(x) for x in tmp_list]
            picture_url_count_file.write("\t".join(tmp_list)+"\n")
        tmp_list = [lv_date, "Total", str(total), str(total)]
        picture_url_count_file.write("\t".join(tmp_list)+"\n")
        picture_url_count_file.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

def getPluginClass():
    return BdTxtCount
