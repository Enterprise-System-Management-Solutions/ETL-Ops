##Internet check/ping count
import paramiko
import re

# Configuration
username = "user"
password = "DDDD"
nodes = ["172.16.2.48","172.16.2.51","172.16.5.114","172.16.5.115","172.16.5.116","172.16.5.117","172.16.5.118","172.16.5.151","172.16.5.16","172.16.5.17","172.16.5.18"
,"172.16.5.19","172.16.5.33","172.16.5.51","172.16.5.52","172.16.5.53","172.16.5.54","172.16.5.58","172.16.5.71","172.16.5.72","172.16.5.73","172.16.5.74","172.16.5.75"
,"172.16.5.76","172.16.5.77","172.16.5.78","172.16.5.79","172.16.5.80","172.16.5.81","172.16.5.82","172.16.5.83","172.16.5.84","172.16.5.85","172.16.5.86","172.16.5.87"
,"172.16.5.88","172.16.5.89","172.16.5.90","172.16.5.91","172.16.5.92","172.16.5.93","172.16.7.118"]  # Replace with your actual node names
ip_address = "8.8.8.8"  # Replace with the IP address you want to ping
ping_count= 4

# SSH connection function
def ssh_connect(hostname):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=username, password=password)
    return client

# Function to check mount point size
def execute_command(client, cmd):
    stdin, stdout, stderr = client.exec_command(cmd)
    output = stdout.readlines()
    error = stderr.readlines()
    return output, error

# Main script
if __name__ == "__main__":
    for node in nodes:
        print(f"Node: {node}")
        try:
            client = ssh_connect(node)
            ping_command = f"ping -c {ping_count} {ip_address}"
            output, error = execute_command(client, ping_command)

            if len(output) >= ping_count:
                success_count = len(re.findall("bytes from", "".join(output)))
                if success_count == ping_count:
                                print("Okay")
                else:
                    print("Ping failed: Not all pings were successful")
            else:
                print("Ping failed: Insufficient response count")
            client.close()

        except paramiko.AuthenticationException:
            print(f"Failed to connect to {node}: Invalid credentials")
        except paramiko.SSHException:
            print(f"Failed to connect to {node}: SSH connection failed")
        except paramiko.ssh_exception.NoValidConnectionsError:
            print(f"Failed to connect to {node}: No valid connections")

