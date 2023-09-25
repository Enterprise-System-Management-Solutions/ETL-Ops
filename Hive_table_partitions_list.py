"""
Created on Sun Jun 27 13:02:38 2023

@author: khairul
"""

import os
import time
import subprocess
from datetime import datetime, timedelta

JDBC="jdbc:hive2://gzplr1cdpmas01.banglalink.net:2181,gzplr2cdpmas02.banglalink.net:2181,gzplr3cdpmas03.banglalink.net:2181/default;principal=hive/_HOST@BANGLALINK.NET;serviceDiscoveryMode=zooKeeper;ssl=true;sslTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks;zooKeeperNamespace=hiveserver2"

dtobj=datetime.today()
dt=dtobj.strftime('%Y%m%d')
dtt=dtobj.strftime('%Y-%m-%d')
#dt='20220623'

##rowcount_part=os.system('beeline -u "{}" -e "select count(*) from layer3.4g_subs_traffic_fact where processing_date=\'{}\';"'.format(JDBC,dtt))

def execute_command(input_file, output_file, command_template):
    with open(input_file, 'r') as input_f, open(output_file, 'w') as output_f:
        for line in input_f:
            # Remove the newline character from the line
            line = line.strip()
            
            # Construct the command by replacing '{line}' in the template with the actual line content
            command = os.system('beeline -u "{}" -e "show partitions {};"'.format(JDBC,line))
            
            # Execute the command and capture the output
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
            
            # Write the output to the output file
            output, _ = process.communicate()
            output_f.write(output)

# Example usage
input_file = '/home/airflow/DevOps/td_backup_object_list.txt'
output_file = '/home/airflow/DevOps/td_partitions_output.txt'
command_template = os.system('show partitions ')

execute_command(input_file, output_file, command_template)

"""
if rowcount_part>1000:
    print('Current Date:\'{}\' is already exists. Please check manually'.format(dtt))
    os.system('echo "data loading status:4g_subs_traffic_fact" | mailx -s "Layer3.4g_subs_traffic_fact refresh status\'{}\' : Failed" mdkhasan@banglalink.net'.format(dtt))
else:
    os.system('beeline -u "{}" -e "ventdate,substr(eventtimestamp,12,2);"'.format(JDBC,dtt))
    os.system('beeline -u "{}" -e "select count(*) from layer3.4g_subs_traffic_fact where processing_date=\'{}\';"'.format(JDBC,dtt))
    mail_s = 'Dear Team,\n4g_subs_traffic_fact is done. Please check.\n\nThanks,\nTeam BDS\n\n\nThis is system generated mail. Please do not reply.'
    os.system('echo "data loading status:4g_subs_traffic_fact is done\n\n\n {}" | mailx -r mdkhasan@banglalink.net -s "Layer3.4g_subs_traffic_fact refresh status\'{}\' " mdkhasan@banglalink.net'.format(mail_s,dtt))
"""
