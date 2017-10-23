#!/bin/bash

cur=$(dirname $0)
cur=$(cd $cur && cd ../ &&  pwd)

now_date=$1
lv_date=`date -d "$now_date" +"%Y%m%d"`
log_date=`date -d "$now_date 1 days ago" +"%Y%m%d"`

cd $cur/script

hadoopsw="/"

sh do.sh 20 10g do.py $lv_date $log_date $cur $hadoopsw

sh move_result.sh $lv_date $cur

