import paramiko

# Configuration
username = "cdpadm"
password = "Dhaka_Bangla!123"
nodes = ["172.16.2.48","172.16.2.51","172.16.5.114","172.16.5.115","172.16.5.116","172.16.5.117","172.16.5.118","172.16.5.151","172.16.5.16","172.16.5.17","172.16.5.18"
,"172.16.5.19","172.16.5.33","172.16.5.51","172.16.5.52","172.16.5.53","172.16.5.54","172.16.5.58","172.16.5.71","172.16.5.72","172.16.5.73","172.16.5.74","172.16.5.75"
,"172.16.5.76","172.16.5.77","172.16.5.78","172.16.5.79","172.16.5.80","172.16.5.81","172.16.5.82","172.16.5.83","172.16.5.84","172.16.5.85","172.16.5.86","172.16.5.87"
,"172.16.5.88","172.16.5.89","172.16.5.90","172.16.5.91","172.16.5.92","172.16.5.93","172.16.7.118"]  # Replace with your actual node names
script_path = "/home/airflow/upgradation/agent_backup.sh"  # Replace with the actual path of the shell script

# SSH connection function
def ssh_connect(hostname):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=username, password=password)
    return client

# Function to copy script to remote node
def copy_script(client, script_path, remote_path):
    sftp = client.open_sftp()
    sftp.put(script_path, remote_path)
    sftp.close()

# Function to execute script remotely
def execute_script(client, script_path):
    stdin, stdout, stderr = client.exec_command(f"bash {script_path}")
    output = stdout.read().decode().strip()
    error = stderr.read().decode().strip()
    if error:
        return error
    return output

# Main script
if __name__ == "__main__":
    try:
        for node in nodes:
            print(f"Node: {node}")
            try:
                client = ssh_connect(node)
                remote_script_path = "/tmp/agent_backup.sh"  # Destination path on the remote node
                copy_script(client, script_path, remote_script_path)
                result = execute_script(client, remote_script_path)
                if result == "Okay":
                    print("Execution successful")
                else:
                    print(f"Execution failed: {result}")
                client.close()
            except paramiko.AuthenticationException:
                print(f"Failed to connect to {node}: Invalid credentials")
            except paramiko.SSHException:
                print(f"Failed to connect to {node}: SSH connection failed")
            except paramiko.ssh_exception.NoValidConnectionsError:
                print(f"Failed to connect to {node}: No valid connections")
    except FileNotFoundError:
        print("Script file not found")
