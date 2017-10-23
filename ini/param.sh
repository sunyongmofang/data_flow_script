#!/bin/bash

mysql_address="101.200.3.217"
mysql_user="root"
mysql_passwd="hongju0908"
mysql_database="bd_stat"

LV_DATE=$1
ROOT_DIR=$2

ARCHIVE_ROOT="$ROOT_DIR/archive"
TMP_ROOT="$ROOT_DIR/tmp"
RESULT_ROOT="$ROOT_DIR/result"
BACKUP_ROOT="$ROOT_DIR/backup"

ARCHIVE_DIR="$ARCHIVE_ROOT/$LV_DATE"
TMP_DIR="$TMP_ROOT/$LV_DATE"
BACKUP_DIR="$BACKUP_ROOT/$LV_DATE"
APPEND_DIR="$APPEND_ROOT/$LV_DATE"
