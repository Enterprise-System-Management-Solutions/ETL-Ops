import subprocess

def delete_hdfs_directories(directory_list):
    for directory in directory_list:
        hdfs_command = f'hdfs dfs -rm -r -skipTrash {directory}'
        subprocess.run(hdfs_command, shell=True)

def dir_list(dir):


# Example usage
directories_to_delete = ['/user/hadoop/dir1', '/user/hadoop/dir2', '/user/hadoop/dir3']
delete_hdfs_directories(directories_to_delete)
