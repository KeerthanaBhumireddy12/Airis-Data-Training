#! /bin/bash
. /home/hduser/finalproject/properties.config

hive<<EOF
use ${hive_database};
create external table if not exists ${table_name}(date_entry string,country string,confirmed int,deaths int,recovered int) row format delimited fields terminated by ',';
load data inpath '/user/hduser/finalproject/Corona.csv' into table ${table_name};
create external table if not exists ${partition_table}(date_entry string,confirmed int,deaths int,recovered int) partitioned by(country string) row format delimited fields terminated by ',';
insert overwrite table ${partition_table} partition(country) select date_entry,country,confirmed,deaths,recovered from ${table_name};


