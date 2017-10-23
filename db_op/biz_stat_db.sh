#!/bin/bash

LV_DATE=$1
ROOT_PATH=$2

. ../ini/param.sh $LV_DATE $ROOT_PATH

mkdir -p $TMP_DIR

if [ -f $TMP_DIR/jk_bdname_map ];then
    random=`date +%s`
    mv $TMP_DIR/jk_bdname_map $TMP_DIR/jk_bdname_map.$random
fi

if [ -f $TMP_DIR/sid_income_map ];then
    random=`date +%s`
    mv $TMP_DIR/sid_income_map $TMP_DIR/sid_income_map.$random
fi

mysql -h $mysql_address -u$mysql_user -p$mysql_passwd $mysql_database -N -e "set names gb2312;select b.hj_sid,a.bdname from jk_bdname_map a, pid_sid_map b where a.jk_account=b.jk_account;"| awk -F"\t" '{OFS="\3"; print $1,$2,0;}' > $TMP_DIR/jk_bdname_map

mysql -h $mysql_address -u$mysql_user -p$mysql_passwd $mysql_database -N -e "select hj_sid,c_income,o_income,hj_pid, jk_account, jk_pid, jk_sid, baidu_id, m_rate, comment, exhibit_pv_rate, exhibit_clk_rate, q_value, debug_url from biz_stat_all_jk where date='$LV_DATE';"| awk -F"\t" '{OFS="\3"; print $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14;}' > $TMP_DIR/sid_income_map

vim "+set fileencoding=utf-8" $TMP_DIR/jk_bdname_map << EOF
:wq
EOF
