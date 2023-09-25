import paramiko
import re
# Configuration
username = "user"
password = "user!123"
nodes = ["172.16.5.16","172.16.5.17","172.16.5.18","172.16.5.58","172.16.5.71","172.16.5.72","172.16.5.73","172.16.5.74","172.16.5.75","172.16.5.76","172.16.5.77","172.16.5.78","172.16.5.79","172.16.5.80","172.16.5.81","172.16.5.82","172.16.5.83","172.16.5.84","172.16.5.85","172.16.5.86","172.16.5.87"]  # Replace with your actual node names
command = "ll /data01/yarn/nm/filecache/"
#grep_pattern = "Active:"

# SSH connection function
def ssh_connect(hostname):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=username, password=password)
    return client

# Function to check mount point size
def execute_command_and_grep(client, cmd):
    stdin, stdout, stderr = client.exec_command(cmd)
    output = stdout.readlines()
    if output:
        for line in output:
            print(line)
    else:
        print("No output")
# Main script
if __name__ == "__main__":
    for node in nodes:
        print(f"Node: {node}")
        try:
            client = ssh_connect(node)
            execute_command_and_grep(client, command)
            client.close()
        except paramiko.AuthenticationException:
            print(f"Failed to connect to {node}: Invalid credentials")
        except paramiko.SSHException:
            print(f"Failed to connect to {node}: SSH connection failed")
        except paramiko.ssh_exception.NoValidConnectionsError:
            print(f"Failed to connect to {node}: No valid connections")