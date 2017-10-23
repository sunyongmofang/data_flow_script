#!/usr/bin/env python

from operator import add

class PvClickTime:
    def setPlatform(self, platform):
        self.platform = platform

    def start(self):
        bs_pvid = self.get_bs_rdd().map(lambda line:line[3]).collect()
        pvid = set(bs_pvid)
        bs_sidpvid_time_rdd = self.get_bs_rdd().filter(lambda line:line[0].isdigit() and len(line[55]) == 9).map(lambda line: ( "\3".join( [ line[55], line[3] ] ), int(line[0]) ) )
        fs_sidpvid_time_rdd = self.get_fs_rdd().filter(lambda line:line[0].isdigit() and len(line[17]) == 9 and line[10] in pvid).map(lambda line: ( "\3".join( [ line[17], line[10] ] ), int(line[0]) ) )
        fs_rm_repeat_rdd = fs_sidpvid_time_rdd.map(lambda (sid_pvid,tm):(sid_pvid,[tm])).reduceByKey(add).map(lambda (sid_pvid,tms):(sid_pvid,min(tms)))
        pv_click_time_rdd = bs_sidpvid_time_rdd.fullOuterJoin(fs_rm_repeat_rdd).filter(lambda (k, v): None not in v).map(lambda (k,v): ( k.split("\3")[0], v[0]-v[1]  ) )
        deal_range_rdd = pv_click_time_rdd.map(lambda (sid,tm):(sid,tm if tm >= 0 else -1)).map(lambda (sid,tm):(sid,tm/5 if 0<tm<30 else tm)).map(lambda (sid,tm):(sid,tm/10+3 if 30<=tm<100 else tm))
        self.pv_click_time = deal_range_rdd.map(lambda (sid,tm):(sid,tm if tm<100 else 13)).map(lambda (sid,tm):  sid+"\t"+str(tm) ).countByValue()
        self.r_click = self.get_bs_rdd().map(lambda line:line[55]).countByValue()

    def stop(self):
        param = self.platform.param
        pv_click_time = self.pv_click_time
        r_click = self.r_click
        lv_date = self.get_super_var().lv_date
        sid_set = set()

        for k in pv_click_time:
            sid = k.split("\t")[0]
            sid_set.add(sid)

        fp = open(param["result"]+"pv_click_time_div","w")
        for sid in sid_set:
            total = 0
            tmp_list = []
            tmp_list.append(lv_date)
            tmp_list.append(sid)
            for nn in xrange(14):
                key = sid + "\t" + str(nn)
                num = pv_click_time[key] if key in pv_click_time else 0
                tmp_list.append(num)
                total += num
            key = sid + "\t" + "-1"
            err = pv_click_time[key] if key in pv_click_time else 0
            r_total = r_click[sid] if sid in r_click else 0
            err = err + (r_total - total)
            tmp_list.append(err)
            tmp_list.append(r_total)
            tmp_list.append("\n")
            tmp_list = [str(x) for x in tmp_list]
            result_str = "\t".join(tmp_list)
            if total > 50:
                fp.write(result_str)
        fp.close()
        

    def get_fs_rdd(self):
        return self.platform.fs_rdd

    def get_bs_rdd(self):
        return self.platform.bs_rdd

    def get_bp_rdd(self):
        return self.platform.bp_rdd

    def get_super_var(self):
        return self.platform

def getPluginClass():
    return PvClickTime
