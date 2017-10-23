#!/usr/bin/env python

from operator import add

class SpecifyAd:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        adlist_pv = self.get_fs_rdd().filter(lambda line:"00000000" in line[21]).map(lambda line:line[21]).countByValue()
        adlist_click = self.get_bs_rdd().filter(lambda line:"00000000" in line[4]).map(lambda line:line[4]).countByValue()

        self.adlist_pv = adlist_pv
        self.adlist_click = adlist_click

    def stop(self):
        param = self.platform.param
        adlist_pv = self.adlist_pv
        adlist_click = self.adlist_click
        lv_date = self.platform.lv_date

        perfix_ad_list_num_file = open(param["result"]+"perfix_ad_list_num","w")
        for adid in adlist_pv:
            pv_num = adlist_pv[adid]
            click_num = adlist_click.get(adid, 0)
            tmp_list = [lv_date,adid,click_num,pv_num]
            tmp_list = [str(x) for x in tmp_list]
            result_str = "\t".join(tmp_list) + "\n"
            perfix_ad_list_num_file.write(result_str)
        perfix_ad_list_num_file.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return SpecifyAd
