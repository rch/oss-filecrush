#!/bin/bash

BLINE='beeline -u "'"$BEELINE_CONNECT"'"'

DB_NAME=aarau1
set -u
LINES=$1

$BLINE <<EOF
use $DB_NAME;

drop table if exists t_text;
drop table if exists t_sequence;
drop table if exists t_avro;
drop table if exists t_snappy_avro;
drop table if exists t_parquet;
drop table if exists t_snappy_parquet;
drop table if exists t_orc;

create table t_text like baseline stored as textfile;
create table t_sequence like baseline stored as sequencefile;
create table t_avro like baseline stored as avro;
create table t_snappy_avro like baseline stored as avro;
create table t_parquet like baseline stored as parquet;
create table t_snappy_parquet like baseline stored as parquet;
create table t_orc like baseline stored as orcfile;

set mapreduce.job.reduces=100;
set hive.merge.mapfiles = false;
set hive.merge.mapredfiles = false;

insert overwrite table t_text            select * from (select * from baseline limit $LINES) x distribute by rand();
insert overwrite table t_sequence        select * from (select * from baseline limit $LINES) x distribute by rand();
SET hive.exec.compress.output=false;
insert overwrite table t_avro            select * from (select * from baseline limit $LINES) x distribute by rand();
SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;
insert overwrite table t_snappy_avro     select * from (select * from baseline limit $LINES) x distribute by rand();
set parquet.compression=UNCOMPRESSED;
insert overwrite table t_parquet         select * from (select * from baseline limit $LINES) x distribute by rand();
set parquet.compression=SNAPPY;
insert overwrite table t_snappy_parquet  select * from (select * from baseline limit $LINES) x distribute by rand();
insert overwrite table t_orc             select * from (select * from baseline limit $LINES) x distribute by rand();
EOF
