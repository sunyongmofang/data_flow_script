#!/usr/bin/env python

from operator import add

class StyleAd:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        non_commonweal_pv_rdd = self.get_fs_rdd().filter(lambda line: line[15]=="0" and line[25]=="0" and line[38]=="1")
        self.style_pv = non_commonweal_pv_rdd.map(lambda line:line[17][-4:]).countByValue()
        self.style_click = self.get_bs_rdd().filter(lambda line:line[10]=="3").map(lambda line:line[55][-4:]).countByValue()

    def stop(self):
        param = self.platform.param
        style_pv = self.style_pv
        style_click = self.style_click
        lv_date = self.platform.lv_date

        ad_style_click_rate_file = open(param["result"]+"ad_style_click_rate","w")
        for style in style_pv:
            if len(style) == 4 and style.isdigit() and style[0] == '0':
                pv_num = style_pv[style]
                click_num = style_click.get(style, 0)
                tmp_list = [lv_date,style,click_num,pv_num]
                tmp_list = [str(x) for x in tmp_list]
                result_str = "\t".join(tmp_list) + "\n"
                ad_style_click_rate_file.write(result_str)
        ad_style_click_rate_file.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return StyleAd
