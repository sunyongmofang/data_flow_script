#!/usr/bin/env python

from operator import add

class BizStatJK:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        sid_white_list = ["219000101", "252010101", "252020101", "252030101", "219080101", "219090101", "219100101", "219110101", "252040101", "252050101", "252060101"]
        self.sid_white_list = sid_white_list
        #bill_log count
        bs_cache_rdd = self.get_bs_rdd().map(lambda line:[ line[1], line[10], line[12], line[13], line[40], line[55] ])
        bs_cache_rdd.cache()

        self.r_click = bs_cache_rdd.map(lambda line:line[5]).countByValue()
        self.hj_click = bs_cache_rdd.filter(lambda line:line[1]=="3").map(lambda line:line[4]).countByValue()
        self.bill_click = bs_cache_rdd.filter(lambda line:line[1]=="3" and line[2]=="0").map(lambda line:line[4]).countByValue()
        self.anti_click = bs_cache_rdd.filter(lambda line:line[1]=="3" and line[2]=="0" and line[3]=="0").map(lambda line:line[4]).countByValue()

        self.r_click_nzz = bs_cache_rdd.filter(lambda line:"900" == line[4][:3] or line[4] in sid_white_list).map(lambda line:line[4]).countByValue()

        self.ip_click_num = bs_cache_rdd.map(lambda line:"\3".join([ line[5], line[0] ])).distinct().map(lambda line:line.split("\3")[0]).countByValue()

        bs_cache_rdd.unpersist()

        #fs_log_1 count
        url_lvl_checkout_rdd = self.get_fs_rdd().filter(lambda line:line[23]=="0" or line[23]=="1").map(lambda line:[ line[4], line[10], line[15], line[17], line[25], line[38] ])
        url_lvl_checkout_rdd.cache()

        self.r_pv = url_lvl_checkout_rdd.map(lambda line:line[3]).countByValue()
        self.no_req_bd = url_lvl_checkout_rdd.filter(lambda line:line[5]=="0").map(lambda line:line[3]).countByValue()
        self.valid_pv = url_lvl_checkout_rdd.filter(lambda line:line[5]=="1").map(lambda line:line[3]).countByValue()
        self.noncommweal_pv = url_lvl_checkout_rdd.filter(lambda line:line[5]=="1" and line[2]=="0" and line[4]=="0").map(lambda line:line[0]).countByValue()
        self.noad = url_lvl_checkout_rdd.filter(lambda line:line[5]=="1" and line[2]=="0" and line[4]!="0").map(lambda line:line[3]).countByValue()
        self.xml_error = url_lvl_checkout_rdd.filter(lambda line:line[2]!="0").map(lambda line:line[3]).countByValue()

        self.fs_pv = url_lvl_checkout_rdd.map(lambda line:line[0]).countByValue()
        self.hj_pv = url_lvl_checkout_rdd.map(lambda line:"\3".join([ line[0], line[1] ])).distinct().map(lambda line:line.split("\3")[0]).countByValue()

        self.noad_nzz = url_lvl_checkout_rdd.filter(lambda line:"900" == line[0][:3] or line[0] in sid_white_list).filter(lambda line:line[5]=="1" and line[2]=="0" and line[4]!="0").map(lambda line:line[0]).countByValue()

        url_lvl_checkout_rdd.unpersist()

        #fs_log_2 count
        url_lvl_checkout_rdd = self.get_fs_rdd().filter(lambda line:line[23]=="0" or line[23]=="1").map(lambda line:[ line[2], line[9], line[15], line[17], line[25], line[27], line[38], line[4] ])
        url_lvl_checkout_rdd.cache()

        self.ip_num = url_lvl_checkout_rdd.map(lambda line:"\3".join([ line[3], line[0] ])).distinct().map(lambda line:line.split("\3")[0]).countByValue()
        self.ip_num_nzz = url_lvl_checkout_rdd.filter(lambda line:"900" == line[7][:3] or line[7] in sid_white_list).map(lambda line:"\3".join([ line[7], line[0] ])).distinct().map(lambda line:line.split("\3")[0]).countByValue()
        self.ip_total = url_lvl_checkout_rdd.map(lambda line:line[0]).distinct().count()
        self.huid_num = url_lvl_checkout_rdd.map(lambda line:(line[5], line[3])).groupByKey().mapValues(lambda v:list(v)[0]).values().countByValue()
        self.non_commweal_ip_num = url_lvl_checkout_rdd.filter(lambda line:line[6]=='1' and line[2]=='0' and line[4]=='0').map(lambda line:"\3".join([ line[3], line[0] ])).distinct().map(lambda line:line.split("\3")[0]).countByValue()
        self.empty_refer_num = url_lvl_checkout_rdd.filter(lambda line:line[1]=="").map(lambda line:line[3]).countByValue()

        url_lvl_checkout_rdd.unpersist()

    def stop(self):
        param = self.platform.param
        lv_date = self.platform.lv_date

        ip_total, r_click_nzz, noad_nzz, ip_num_nzz = self.ip_total, self.r_click_nzz, self.noad_nzz, self.ip_num_nzz

        r_click, hj_click, bill_click, anti_click, ip_click_num, r_pv = self.r_click, self.hj_click, self.bill_click, self.anti_click, self.ip_click_num, self.r_pv
        no_req_bd, valid_pv, noncommweal_pv, noad, xml_error, fs_pv = self.no_req_bd, self.valid_pv, self.noncommweal_pv, self.noad, self.xml_error, self.fs_pv
        hj_pv, ip_num, huid_num, non_commweal_ip_num, empty_refer_num = self.hj_pv, self.ip_num, self.huid_num, self.non_commweal_ip_num, self.empty_refer_num

        sid_list, sid_bdname, c_income, o_income = {}, {}, {}, {}
        pid, jk_acc, jk_pid, jk_sid, bd_id, m_rate = {}, {}, {}, {}, {}, {}
        comment, e_pv_rate, e_clk_rate, q_value, debug_url = {}, {}, {}, {}, {}

        e_pv_total, e_click_total = 0, 0

        jk_bdname_map_file = open(param["tmp"]+lv_date+"/jk_bdname_map", "r")
        for line in jk_bdname_map_file:
            ls = line.strip("\n").split("\3")
            sid = ls[0]
            sid_bdname[sid] = ls[1]
        jk_bdname_map_file.close()

        sid_income_map_file = open(param["tmp"]+lv_date+"/sid_income_map", "r")
        for line in sid_income_map_file:
            ls = line.strip("\n").split("\3")
            sid = ls[0]
            c_income[sid] = ls[1]
            o_income[sid] = ls[2]
            if sid == "Total":
                continue
            pid[sid], jk_acc[sid], jk_pid[sid], jk_sid[sid], bd_id[sid], m_rate[sid] = ls[3], ls[4], ls[5], ls[6], ls[7], ls[8]
            comment[sid], e_pv_rate[sid], e_clk_rate[sid], q_value[sid], debug_url[sid] = ls[9], ls[10], ls[11], ls[12], ls[13]
            sid_list[sid] = sid
        sid_income_map_file.close()

        result_list1 = [ pid, sid_list, jk_acc, jk_pid, jk_sid, hj_pv, hj_click, bill_click, anti_click, {}, noad, {}, fs_pv, ip_num, c_income, o_income ]
        result_list2 = [ bd_id, m_rate, huid_num, comment, anti_click, r_pv, e_pv_rate, {}, {}, r_click, hj_click, e_clk_rate ]
        result_list3 = [ ip_click_num, non_commweal_ip_num, sid_bdname, debug_url, q_value, no_req_bd, empty_refer_num, valid_pv, noncommweal_pv, xml_error ]

        sum_map = lambda m:sum( [ m[x] for x in m ] )

        result_list1_total = ["Total", "Total", "", "", "", sum_map(hj_pv), sum_map(hj_click), sum_map(bill_click), sum_map(anti_click), 0, sum_map(noad), 0, sum_map(fs_pv), ip_total, c_income.get("Total", 0), o_income.get("Total", 0)]
        result_list2_total = [ "", "", sum_map(huid_num), "", sum_map(anti_click), sum_map(r_pv), 0, 0, 0, sum_map(r_click), sum_map(hj_click), 0 ]
        result_list3_total = [ sum_map(ip_click_num), sum_map(non_commweal_ip_num), "", "", 0, sum_map(no_req_bd), sum_map(empty_refer_num), sum_map(valid_pv), sum_map(noncommweal_pv), sum_map(xml_error) ]

        biz_stat_all_jk_file = open(param["result"]+"biz_stat_all_jk", "w")

        for sid in sid_list:
            tmp_list = [lv_date]
            result_num1 = [x.get(sid, 0) for x in result_list1]
            result_num2 = [x.get(sid, 0) for x in result_list2]
            if result_num1[12] == 0 and result_num2[5] == 0:
                continue
            result_num3 = [x.get(sid, 0) for x in result_list3]

            if sid[:3] == "900" or sid in self.sid_white_list:
                result_num2[9] = r_click_nzz.get(sid, 0)
                result_num2[5] = result_num1[12]
                result_num1[10] = noad_nzz.get(sid, 0)
                result_num1[13] = ip_num_nzz.get(sid, 0)

            e_pv = result_num3[8] + (result_num1[10] * float(result_num2[6]))
            e_click = result_num2[4] + ( (result_num2[9] - result_num1[6]) * float(result_num2[11]) )

            e_pv_total += e_pv
            e_click_total += e_click

            result_num2[7], result_num2[8] = int(e_pv), int(e_click)

            tmp_list += result_num1
            tmp_list += result_num2
            tmp_list += result_num3
            tmp_list = [str(x) for x in tmp_list]
            biz_stat_all_jk_file.write("\t".join(tmp_list)+"\n")

        result_list2_total[7], result_list2_total[8] = int(e_pv_total), int(e_click_total)
        tmp_list = [lv_date]
        tmp_list += result_list1_total
        tmp_list += result_list2_total
        tmp_list += result_list3_total
        tmp_list = [str(x) for x in tmp_list]
        biz_stat_all_jk_file.write("\t".join(tmp_list)+"\n")

        biz_stat_all_jk_file.close()

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

def getPluginClass():
    return BizStatJK
