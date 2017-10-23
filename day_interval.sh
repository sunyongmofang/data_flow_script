#!/bin/bash

date1=$1
num=$2

for(( i=0;$i<=$num;i++ ))
do

    day_tmp=`date -d"$date1 $i days ago" +"%Y%m%d"`
    echo $day_tmp
    sh tmp.sh $day_tmp

done
