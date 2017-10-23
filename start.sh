#!/bin/bash

cur=$(dirname $0)
cur=$(cd $cur && cd ../ &&  pwd)

now_date=`date`
lv_date=`date -d "$now_date" +"%Y%m%d"`
log_date=`date -d "$now_date 1 days ago" +"%Y%m%d"`

#jesgoo log check
hadoopdate=`date -d"$log_date" +"%F"`
linenum=`/home/sunyong/hadoop/bin/hadoop fs -ls /log/fs/ie_log/$hadoopdate/*/* | wc -l`

checknum=`ssh worker@101.200.3.217 "ls /search/worker/log_center/data/frontserver/ie_log/$log_date | wc -l"`
checknum=`echo $checknum|awk '{print $1*2-12}'`

if (($linenum < $checknum));then
    echo "$linenum < $checknum error"
    exit 1
fi
#check end

mkdir -p $cur/tmp/$lv_date

cd $cur/script

cd db_op
sh biz_stat_db.sh $lv_date $cur
cd - > /dev/null

successnum=`/home/sunyong/hadoop/bin/hdfs dfs -ls /user/sunyong/log/fs/ie_log/$hadoopdate/*/* | grep "_SUCCESS" | wc -l`

[ $successnum == 288 ] && hadoopsw="/user/sunyong/" || hadoopsw="/"
echo $hadoopsw

sh do.sh 20 10g do.py $lv_date $log_date $cur $hadoopsw

sh move_result.sh $lv_date $cur

