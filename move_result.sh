#!/bin/bash

LV_DATE=$1
ROOT_PATH=$2

. ./ini/param.sh $LV_DATE $ROOT_PATH

mkdir -p $ARCHIVE_DIR
mkdir -p $BACKUP_DIR

mv $RESULT_ROOT/* $ARCHIVE_DIR

cd $ARCHIVE_DIR

random=`date +%s`

for i in `ls`
do
    mysql -h $mysql_address -u$mysql_user -p$mysql_passwd $mysql_database -N -e "select * from $i where date='$LV_DATE';" > $BACKUP_DIR/$i.$random
    mysql -h $mysql_address -u$mysql_user -p$mysql_passwd $mysql_database -N -e "delete from $i where date='$LV_DATE';"
    /usr/bin/mysqlimport -r --host=$mysql_address --user=$mysql_user --password=$mysql_passwd --local $mysql_database $i
done

cd - > /dev/null

mysql -h $mysql_address -u$mysql_user -p$mysql_passwd $mysql_database -N -e "update biz_stat_all_jk set debug_url='' where date='$LV_DATE' and debug_url='NULL';"
