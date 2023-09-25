import os
import time
from datetime import datetime, timedelta

JDBC="jdbc:hive2://gzplr1cdpmas01.banglalink.net:2181,gzplr2cdpmas02.banglalink.net:2181,gzplr3cdpmas03.banglalink.net:2181/default;principal=hive/_HOST@BANGLALINK.NET;serviceDiscoveryMode=zooKeeper;ssl=true;sslTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks;zooKeeperNamespace=hiveserver2"

part=int(time.time())
dtobj=datetime.today()
dt=dtobj.strftime('%Y%m%d')
dtobj_f=dtobj.strftime('%Y-%m-%d')
##dt='20220703'

filename='/data02/edw/subscription_snapshot_fct_{}.csv'.format(dt)
print('filename - '+filename)
rowcnt = int(os.popen('cat {} | wc -l'.format(filename)).read()) if os.path.exists(filename) else 0
os.system('cat {} | wc -l'.format(filename))

if os.path.exists(filename) and rowcnt>1000:
    print('deleting file from /prod.db/lookups/subscription_snapshot_fct_csv/')
    os.system('hdfs dfs -rm -f -skipTrash /prod.db/lookups/subscription_snapshot_fct_csv/*')
    print('putting file into /prod.db/lookups/subscription_snapshot_fct_csv/')
    os.system('hdfs dfs -put {} /prod.db/lookups/subscription_snapshot_fct_csv/'.format(filename))

    os.system('beeline -u "{}" -e "drop table if exists lookups.subscription_snapshot_fct_tmp purge"'.format(JDBC))
    os.system("beeline -u \"{}\" -e \"CREATE EXTERNAL TABLE lookups.subscription_snapshot_fct_tmp(date_key string, subscription_key string, product_key string, geography_working_key string, geography_non_working_key string, geography_general_key string, geography_first_call_key string, days_since_last_activity string, first_recharge_date_key string, first_recharge_amount string, data_activity_status_key string, data_act_stat_change_type_key string, first_data_activity_date_key string, rga_activity_status_key string, rga_abs_change_type_key string, first_rga_date_key string, biometric_activity_change_key string, first_data_4g_act_date_key string, data_4g_activity_status_key string, data_4g_act_stat_chng_type_key string) \
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' \
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' \
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' \
        LOCATION 'hdfs://NameNodeHA/prod.db/lookups/subscription_snapshot_fct_{}' \
        TBLPROPERTIES ( 'external.table.purge'='true', 'parquet.compression'='snappy');\"".format(JDBC,part))

    os.system('beeline -u "{}" -e "insert into lookups.subscription_snapshot_fct_tmp select * from lookups.subscription_snapshot_fct_csv"'.format(JDBC))
    os.system('beeline -u "{}" -e "alter table lookups.subscription_snapshot_fct rename to lookups.subscription_snapshot_fct_old"'.format(JDBC))
    os.system('beeline -u "{}" -e "alter table lookups.subscription_snapshot_fct_tmp rename to lookups.subscription_snapshot_fct"'.format(JDBC))
    os.system('beeline -u "{}" -e "drop table if exists lookups.subscription_snapshot_fct_old purge"'.format(JDBC))
    os.system('beeline -u "{}" -e "insert into lookups.historical_SSF select DATE_KEY,SUBSCRIPTION_KEY,PRODUCT_KEY,GEOGRAPHY_WORKING_KEY,GEOGRAPHY_NON_WORKING_KEY,GEOGRAPHY_GENERAL_KEY,GEOGRAPHY_FIRST_CALL_KEY,DAYS_SINCE_LAST_ACTIVITY,FIRST_RECHARGE_DATE_KEY,FIRST_RECHARGE_AMOUNT,DATA_ACTIVITY_STATUS_KEY,DATA_ACT_STAT_CHANGE_TYPE_KEY,FIRST_DATA_ACTIVITY_DATE_KEY,RGA_ACTIVITY_STATUS_KEY,RGA_ABS_CHANGE_TYPE_KEY,FIRST_RGA_DATE_KEY,BIOMETRIC_ACTIVITY_CHANGE_KEY,FIRST_DATA_4G_ACT_DATE_KEY,DATA_4G_ACTIVITY_STATUS_KEY,DATA_4G_ACT_STAT_CHNG_TYPE_KEY,''{}'' from lookups.subscription_snapshot_fct_csv;"'.format(JDBC, dtobj_f))
    
    os.system('echo "Data loading done" | mailx -s "subscription_snapshot_fct refresh status : done" bigdataservices@banglalink.net')
    t_r_cnt=os.popen('beeline --showheader=false --outputformat=tsv2 -u "{}" -e "select count(*) from lookups.subscription_snapshot_fct"'.format(JDBC)).read()
    os.system("mysql -h172.16.5.34 -umonitor -pBldmp@123 -e \"insert into monitoring.file_status(dt, source, f_count, f_row_count, t_row_count, status) values('{}','{}','{}','{}','{}','{}')\"".format(dt, 'subscription_snapshot_fct', 1, rowcnt, t_r_cnt,'Data_loaded_succesfully'))
else:
    #mail_s='Dear Team,\nDMP has not received $file for $dt. Please check.\n\nThanks,\nTeam BDS\n\n\nThis is system generated mail. Please do not reply.'
    os.system('echo "Data loading failed for subscription_fct_{}" | mailx -r bigdataservices@banglalink.net -s "File row count issue for susbcription fact" bigdataservices@banglalink.net'.format(dt))

