import pymysql
import os
import time
import datetime
from datetime import date, timedelta
import teradatasql
from contextlib import closing
import subprocess
import ssl
from impala.dbapi import connect


###### Config
sqoop_log_loc="/data01/TD_Backup/log/"


###### MySQL connection
host="172.16.5.34"
user="monitor"
password="Bldmp@123"
database="monitoring"

###### TD Connection
td_host="172.16.5.206"
td_connection="jdbc:teradata://172.16.5.206/DATABASE=dp_tab"
td_uaername="up_etl02"
td_password="Up_etl02_123"
hs2_url="jdbc:hive2://gzplr1cdpmas01.banglalink.net:2181,gzplr2cdpmas02.banglalink.net:2181,gzplr3cdpmas03.banglalink.net:2181/default;princil=hive/_HOST@BANGLALINK.NET;serviceDiscoveryMode=zooKeeper;ssl=true;sslTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks;zooKeeperNamespace=hiveserver2"
hive_user="airflow"
hs2_keytab="/etc/security/keytabs/airflow.headless.keytab"

##### Impala Connection
impala_host='gzplr2cdpmas02.banglalink.net'
impala_port=21050
impala_ssl=True
impala_db='td_backup'
impala_user='airflow'
impala_krb_service_name='impala'
impala_auth='GSSAPI'


###### Functions

#Backup progress
def backup_progress(tab,progress):
    conn_p = pymysql.connect( host=host, user=user, passwd=password, db=database )
    cur_p = conn_p.cursor()
    try:
        comm="truncate table monitoring.td_backup_progress"
        print(comm)
        cur_p.execute(comm)
        comm="insert into monitoring.td_backup_progress values('"+tab+"',"+progress+")"
        print(comm)
        cur_p.execute(comm)
        cur_p.execute("commit")
    except Exception as e :
        print(str(e))
    finally:
        conn_p.close()
    
    

#Initiate Spark
def impala_init():
    conn = connect(host=impala_host, port=impala_port, use_ssl=impala_ssl, database=impala_db, user=impala_user, kerberos_service_name=impala_krb_service_name, auth_mechanism=impala_auth)
    return conn 

#Get date key for yesterday
def get_date_key():
    dt=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    tab_sql = "select date_key from dp_tab.date_dim where date_value='"+dt+"'"
    with teradatasql.connect (host=td_host, user=td_uaername, password=td_password) as con:
        with closing(con.cursor()) as cur:
            cur.execute(tab_sql)
            a = cur.fetchall()
            return str(a[0]).replace('[','').replace(']','')
        
#TD row count
def TD_row_count(db,tab,ld):
    if "N" in ld:
        dt_key=get_date_key()
        tab_sql = "SELECT count(1) from "+db+"."+tab+" where date_key="+dt_key
        with teradatasql.connect (host=td_host, user=td_uaername, password=td_password) as con:
            with closing(con.cursor()) as cur:
                cur.execute(tab_sql)
                a = cur.fetchall()
        return str(a[0]).replace('[','').replace(']','')
    else:
        tabs = tab.split(";")
        cnt = 0
        for t in tabs:
            tab_sql = "SELECT count(1) from "+db+"."+t
            with teradatasql.connect (host=td_host, user=td_uaername, password=td_password) as con:
                with closing(con.cursor()) as cur:
                    cur.execute(tab_sql)
                    a = cur.fetchall()
                    cnt = cnt + int(str(a[0]).replace('[','').replace(']',''))
        return str(cnt)

#Hive row count
def Hive_row_count(owner,tab,ld_tab,extra,is_main):
    #extra to set where condition
    conn=impala_init()
    cur = conn.cursor()
    cnt=0
    
    if "Y" in is_main:
        cmd='select count(1) from '+owner+'.'+tab+' '+extra+';'
        print(cmd)
        cur.execute(cmd)
        result=cur.fetchone() 
        return str(result[0])
    
    if not ld_tab:
        cmd='select count(1) from '+owner+'.'+tab+' '+extra+';'
        print(cmd)
        cur.execute(cmd)
        result=cur.fetchone() 
        return str(result[0])
    else:
        lds=ld_tab.split(";")
        x=1
        for i in lds:
            cmd='select count(1) from '+owner+'.'+tab+'_'+str(x)+' '+extra+';'
            print(cmd)
            cur.execute(cmd)
            result=cur.fetchone() 
            cnt=cnt+int(str(result[0]))
            x=x+1
        conn.close()
        return str(cnt)

#Load data into main table
def load_main_tab(db,tab,tmp_tab,ld_tabs,tab_type,dt,key,cols):
    print("Loading into main table")
    ld_tabs1=ld_tabs.split(";")
    conn=impala_init()
    cur = conn.cursor()
    if "DIM" in tab_type :
        cmd='insert into '+db+'.'+tab+' PARTITION(processing_date) select t.*,"'+dt+'" from '+db+'.'+tmp_tab+' t where '+key+' is not NULL';
        print(cmd)
        cur.execute(cmd)
    elif "FCT" in tab_type :
        if not ld_tabs:
            cmd='insert into '+db+'.'+tab+'('+cols+') PARTITION(processing_date) select '+cols+',"'+dt+'" from '+db+'.'+tmp_tab+' t  where '+key+' is not NULL'
            print(cmd)
            cur.execute(cmd)
        else:
            x=1
            for ld_tab in ld_tabs1:
                cmd='insert into '+db+'.'+tab+'('+cols+') PARTITION(processing_date) select '+cols+',"'+dt+'" from '+db+'.'+tmp_tab+'_'+str(x)+' t  where '+key+' is not NULL'
                print(cmd)
                cur.execute(cmd)
                x=x+1
            
    #print(cmd)
    conn.close()

     
    
    
#Drop temp tables
def drop_temp_tab(db,tab,ld_tabs):
    ld_tabs1=ld_tabs.split(";")
    conn=impala_init()
    cur = conn.cursor()
    
    print(ld_tabs)
    if not ld_tabs:
        cmd='DROP TABLE IF EXISTS '+db+'.'+tab+' purge'
        print(cmd)
        cur.execute(cmd) 
    else:
        x=1
        for ld_tab in ld_tabs1:
            cmd='DROP TABLE IF EXISTS '+db+'.'+tab+'_'+str(x)+' purge'
            print(cmd)
            cur.execute(cmd)    
            x=x+1
    conn.close()
    

#Throughput and applicationid
def log_process(log_file):
    file1 = open(log_file, 'r')
    lines = file1.readlines()
    
    throughput=""
    applicationId=""

    for l in lines :
        if "ImportJobBase" in l :
            if "seconds " in l:
                t=l.split(sep="(")
                t1=t[1].split(sep=")")
                throughput=throughput+";"+str(t1[0])
        if "The url to track the job" in l:
            x=l.split(sep="/")
            applicationId=applicationId+";"+x[6]

    return throughput[1:],applicationId[1:]

#Add regex if table has address
def address_qry(cols):
    x = cols.split(",")
    length = len(x)
    for i in range(length):
        if "ADDRESS" in x[i]:
            t="REGEXP_REPLACE ("+x[i]+", '\\n',' ') as "+x[i]
            x[i]=t
    
    col=",".join(x) 
    return col
            



####### Main Function: Start
def main():
    
    dt=date.today().strftime("%Y-%m-%d")
    
    conn = pymysql.connect( host=host, user=user, passwd=password, db=database )
    cur = conn.cursor()
    cur.execute("select * from monitoring.td_backcup_config")
    myresult = cur.fetchall()

    extra_args=""

    for x in myresult:
        
        TD_TAB_OWNER1=str(x[0])
        TD_TAB_NAME1=str(x[1])
        TD_TAB_TYPE1=str(x[2])
        TD_LD_TAB_OWNER1=str(x[3])
        TD_LD_TAB_NAMES=str(x[4])
        TD_LD_TAB_NAME1=TD_LD_TAB_NAMES.split(";")
        HIVE_TAB_OWNER1=str(x[5])
        HIVE_TAB_NAME1=str(x[6])
        HIVE_TEMP_TAB_NAME1=str(x[7])
        RETENTION1=str(x[8])
        MAPPER1=str(x[9])
        FETCH_SIZE1=str(x[10])
        COMPRESS1=str(x[11])
        SPLIT_BY1=str(x[12])
        TARGET_DIR1=str(x[13])
        INPUT_METHOD1=str(x[14])
        SKIP1=str(x[15])
        COLUMNS1=str(x[16])
        HAS_ADDRESS1=str(x[17])
        
        print("Processing "+TD_TAB_OWNER1+"."+TD_TAB_NAME1)
        print("-----------------------------------------------------")
        
        if TD_TAB_TYPE1 :
            try :
                
                ##### DIM Table Import Start
                               
                # Check if already backup is taken
                print("Checking if backup is already taken.")
                cur1 = conn.cursor()
                cur1.execute('select * from monitoring.td_backcup_feed where DT="'+dt+'" and TD_TAB_OWNER="'+TD_TAB_OWNER1+'" and TD_TAB_NAME="'+TD_TAB_NAME1+'"')
                row_chk=cur1.fetchone() 
                #print(row_chk)
                if row_chk:
                    print("Skipping as backup is already taken.")
                    print()
                    continue


                #Check if Skip parameter is true
                if SKIP1 is "Y" :
                    print("Skipping as skip parameter is true in the config.")
                else :
                    if COMPRESS1 is "Y" :
                        extra_args=extra_args+" --compress"
                    
                    #Clear all temp tables
                    print("Dropping all temporary tables.")
                    drop_temp_tab(HIVE_TAB_OWNER1,HIVE_TEMP_TAB_NAME1,TD_LD_TAB_NAMES)
                    cmd_drp_hdfs="hdfs dfs -rm -r -f "+TARGET_DIR1+" -skipTrash"
                    os.system(cmd_drp_hdfs)
                    

                    backup_progress(TD_TAB_NAME1,"10")

                    ##Backup Start
                    start_time = time.time()
                    print("Backup started on : %s" % datetime.datetime.now())
                    
                    #Pull data from TD
                    dt_key=get_date_key()
                    
                    dt_log=datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                    log_file=sqoop_log_loc+x[0]+"_"+x[1]+'_'+dt_log+'.txt'
                    if "DIM" in TD_TAB_TYPE1:
                        print("Sqoop Backup Started")
                        sqoop_import=""
                        if "Y" in HAS_ADDRESS1:
                            col=address_qry(COLUMNS1)
                            qry="select "+col+" from "+TD_TAB_OWNER1+"."+TD_TAB_NAME1+" where \$CONDITIONS"
                            sqoop_import='nohup sqoop import --connect '+td_connection+' --username '+td_uaername+' --password '+td_password+' --query "'+qry+'" --num-mappers '+MAPPER1+' --fetch-size '+FETCH_SIZE1+' '+extra_args+' --hive-database '+HIVE_TAB_OWNER1+' --hive-table '+HIVE_TEMP_TAB_NAME1+' --create-hive-table --split-by '+SPLIT_BY1+' --target-dir '+TARGET_DIR1+' --delete-target-dir --hive-import --hs2-url "'+hs2_url+'" --hive-user '+hive_user+'  --hs2-keytab '+hs2_keytab+' --fields-terminated-by "~" -- --schema DP_STG --input-method split.by.partition --as-orcfile >>'+log_file+'  2>&1'
                        else:
                            sqoop_import='nohup sqoop import --connect '+td_connection+' --username '+td_uaername+' --password '+td_password+' --table '+TD_TAB_NAME1+' --num-mappers '+MAPPER1+' --fetch-size '+FETCH_SIZE1+' '+extra_args+' --hive-database '+HIVE_TAB_OWNER1+' --hive-table '+HIVE_TEMP_TAB_NAME1+' --create-hive-table --split-by '+SPLIT_BY1+' --target-dir '+TARGET_DIR1+' --delete-target-dir --hive-import --hs2-url "'+hs2_url+'" --hive-user '+hive_user+'  --hs2-keytab '+hs2_keytab+' --fields-terminated-by "~" -- --input-method '+INPUT_METHOD1+' --as-orcfile >>'+log_file+'  2>&1'
                        print(sqoop_import)
                        os.system(sqoop_import)
                        print("Sqoop Backup Done")
                    elif "FCT" in TD_TAB_TYPE1:
                        if not TD_LD_TAB_NAMES:
                            qry="select * from "+TD_TAB_OWNER1+"."+TD_TAB_NAME1+" where date_key="+dt_key+" and \$CONDITIONS"
                            sqoop_import='nohup sqoop import --connect '+td_connection+' --username '+td_uaername+' --password '+td_password+' --query "'+qry+'" --num-mappers '+MAPPER1+' --fetch-size '+FETCH_SIZE1+' '+extra_args+' --hive-database '+HIVE_TAB_OWNER1+' --hive-table '+HIVE_TEMP_TAB_NAME1+' --create-hive-table --split-by '+SPLIT_BY1+' --target-dir '+TARGET_DIR1+' --delete-target-dir --hive-import --hs2-url "'+hs2_url+'" --hive-user '+hive_user+'  --hs2-keytab '+hs2_keytab+' --fields-terminated-by "~" -- --schema '+TD_LD_TAB_OWNER1+' --input-method '+INPUT_METHOD1+' --as-orcfile >>'+log_file+'  2>&1'
                            print(sqoop_import)
                            os.system(sqoop_import)
                        else:
                            print("Sqoop Backup Started")
                            x=1
                            for ld_tab in TD_LD_TAB_NAME1:
                                print("Processing "+ld_tab)
                                sqoop_import='nohup sqoop import --connect '+td_connection+' --username '+td_uaername+' --password '+td_password+' --table '+ld_tab+' --num-mappers '+MAPPER1+' --fetch-size '+FETCH_SIZE1+' '+extra_args+' --hive-database '+HIVE_TAB_OWNER1+' --hive-table '+HIVE_TEMP_TAB_NAME1+'_'+str(x)+' --create-hive-table --split-by '+SPLIT_BY1+' --target-dir '+TARGET_DIR1+' --delete-target-dir --hive-import --hs2-url "'+hs2_url+'" --hive-user '+hive_user+'  --hs2-keytab '+hs2_keytab+' --fields-terminated-by "~" -- --schema '+TD_LD_TAB_OWNER1+' --input-method '+INPUT_METHOD1+' --as-orcfile >>'+log_file+'  2>&1'
                                x=x+1
                                print(sqoop_import)
                                os.system(sqoop_import)
                            print("Sqoop Backup Done")  
                    else:
                        print("Invalid TD Table Type: "+TD_TAB_TYPE1)
                        continue
                    
                    backup_progress(TD_TAB_NAME1,"50")
                    #To update impala metadata
                    print("Sleeping for 60 sec.......")
                    time.sleep(60)

                    #Hive Count:
                    if not TD_LD_TAB_NAMES and "FCT" in TD_TAB_TYPE1:
                        hive_tmp_rc=Hive_row_count(HIVE_TAB_OWNER1,HIVE_TEMP_TAB_NAME1,TD_LD_TAB_NAMES,"where "+SPLIT_BY1+" is not NULL","N")
                    else:
                        hive_tmp_rc=Hive_row_count(HIVE_TAB_OWNER1,HIVE_TEMP_TAB_NAME1,TD_LD_TAB_NAMES,"where "+SPLIT_BY1+" is not NULL","N")
                    print("HIve row: "+hive_tmp_rc)

                    #TD Count
                    if "DIM" in TD_TAB_TYPE1:
                        td_row_count=TD_row_count(TD_TAB_OWNER1,TD_TAB_NAME1,"")  
                    elif "FCT" in TD_TAB_TYPE1:
                        if not TD_LD_TAB_NAMES:
                            td_row_count=TD_row_count(TD_TAB_OWNER1,TD_TAB_NAME1,"N")
                        else:
                            td_row_count=TD_row_count(TD_LD_TAB_OWNER1,TD_LD_TAB_NAMES,"")  
                    print("TD count: "+td_row_count)

                    backup_progress(TD_TAB_NAME1,"60")
                    
                    comment=""
                    data_missmatch=""
                    hive_main_rc=""
                    #If within range then load into Main table else raise flag
                    if int(td_row_count)-int(hive_tmp_rc) <100 :
                        #Load into main table
                        if "DIM" in TD_TAB_TYPE1:
                            load_main_tab(HIVE_TAB_OWNER1,HIVE_TAB_NAME1,HIVE_TEMP_TAB_NAME1,TD_LD_TAB_NAMES,"DIM",dt,SPLIT_BY1,COLUMNS1)
                        elif "FCT" in TD_TAB_TYPE1:
                            load_main_tab(HIVE_TAB_OWNER1,HIVE_TAB_NAME1,HIVE_TEMP_TAB_NAME1,TD_LD_TAB_NAMES,"FCT",dt,SPLIT_BY1,COLUMNS1)
                        else:
                            print("Invalid TD Table Type: "+TD_TAB_TYPE1)
                            continue                        
                        
                        hive_main_rc=Hive_row_count(HIVE_TAB_OWNER1,HIVE_TAB_NAME1,TD_LD_TAB_NAMES,"where processing_date='"+dt+"'","Y")
                        if int(hive_main_rc) == int(hive_tmp_rc) :
                            comment="Table_loaded_successfully."
                            data_missmatch="N"
                        else :
                            comment="Count_missmatch._Please_check_loading_from_staging_table."
                            data_missmatch="Y"



                    else :
                        comment="TD_vs_Hive_count_missmatch._Sqoop_job_might_failed._Please check."
                        data_missmatch="Y"



                    #THROUGHPUT
                    throughput,applicationId = log_process(log_file)
                    
                    
                    ## Backup END
                    end_time = time.time()
                    print("Backup completed on : %s" % datetime.datetime.now())
                    
                    backup_progress(TD_TAB_NAME1,"100")


                    #Backup Info
                    print("Time taken : %s seconds" % (end_time - start_time))
                    print("Number of rows on teradata : "+td_row_count)
                    print("Number of rows on Hive : "+hive_main_rc)
                    print("Throughput: "+throughput)
                    print("ApplicationId: "+applicationId)
                    print("Comment: "+comment)

                    #Insert information into feed table
                    cur2 = conn.cursor()
                    sql = "INSERT INTO monitoring.td_backcup_feed (dt,TD_TAB_OWNER,TD_TAB_NAME,TD_TAB_TYPE,TD_TAB_COUNT,CDP_TAB_OWNER,CDP_TAB_NAME,CDP_TAB_PARTITION,CDP_TAB_COUNT,START_TIME,END_TIME,TIME_TAKEN,THROUGHPUT,DATA_MISSMATCH,applicationId,Comment) VALUES ('"+str(dt)+"', '"+TD_TAB_OWNER1+"', '"+TD_TAB_NAME1+"', '"+TD_TAB_TYPE1+"', '"+str(td_row_count)+"', '"+HIVE_TAB_OWNER1+"', '"+HIVE_TAB_NAME1+"', '"+str(dt)+"', '"+hive_main_rc+"', '"+str(start_time)+"', '"+str(end_time)+"', '"+str(end_time - start_time)+"', '"+throughput+"', '"+data_missmatch+"', '"+applicationId+"', '"+comment+"')"
                    print(sql)
                    cur2.execute(sql)
                    conn.commit()

                    #Drop temp table
                    drop_temp_tab(HIVE_TAB_OWNER1,HIVE_TEMP_TAB_NAME1,TD_LD_TAB_NAMES)
                    cmd_drp_hdfs="hdfs dfs -rm -r -f "+TARGET_DIR1+" -skipTrash"
                    os.system(cmd_drp_hdfs)
                    backup_progress("","0")


                ###### DIM Table Import End
            
            except Exception as e :
                print ("ERROR: "+str(e))
                #Insert information into feed table
                cur3 = conn.cursor()
                sql = "INSERT INTO monitoring.td_backcup_feed (dt,TD_TAB_OWNER,TD_TAB_NAME,TD_TAB_TYPE,TD_TAB_COUNT,CDP_TAB_OWNER,CDP_TAB_NAME,CDP_TAB_PARTITION,CDP_TAB_COUNT,START_TIME,END_TIME,TIME_TAKEN,THROUGHPUT,DATA_MISSMATCH,applicationId,Comment) VALUES ('"+str(dt)+"', '"+TD_TAB_OWNER1+"', '"+TD_TAB_NAME1+"', '"+TD_TAB_TYPE1+"', '', '"+HIVE_TAB_OWNER1+"', '"+HIVE_TAB_NAME1+"', '', '', '', '', '', '', '', '', '"+str(e)+"')"
                print(sql)
                cur3.execute(sql)
                conn.commit()
                

        else :
            print("Invalid TD Table Type: "+TD_TAB_TYPE1)


        print("\n")
        
####### Main Function: END

####### Call main function
if __name__ == "__main__":
    main()

