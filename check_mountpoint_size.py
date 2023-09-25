
import paramiko

# Configuration
username = "user"
password = "user!123"
nodes = ["172.16.2.48","172.16.2.51","172.16.5.114","172.16.5.115","172.16.5.116","172.16.5.117","172.16.5.118","172.16.5.151","172.16.5.16","172.16.5.17","172.16.5.18","172.16.5.19","172.16.5.33","172.16.5.51","172.16.5.52","172.16.5.53","172.16.5.54","172.16.5.58","172.16.5.71","172.16.5.72","172.16.5.73","172.16.5.74","172.16.5.75","172.16.5.76","172.16.5.77","172.16.5.78","172.16.5.79","172.16.5.80","172.16.5.81","172.16.5.82","172.16.5.83","172.16.5.84","172.16.5.85","172.16.5.86","172.16.5.87","172.16.5.88","172.16.5.89","172.16.5.90","172.16.5.91","172.16.5.92","172.16.5.93","172.16.7.118"]  # Replace with your actual node names

# SSH connection function
def ssh_connect(hostname):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=username, password=password)
    return client

# Function to check mount point size
def check_mountpoint_size(client, mountpoint):
    stdin, stdout, stderr = client.exec_command(f"df -h {mountpoint}")
    output = stdout.readlines()
    if output:
        return output[1].split()[1:]
    else:
        return None

# Main script
if __name__ == "__main__":
    for node in nodes:
        print(f"Node: {node}")
        try:
            client = ssh_connect(node)
            opt_size = check_mountpoint_size(client, "/opt")
            tmp_size = check_mountpoint_size(client, "/tmp")
            if opt_size:
                print(f"/opt Size: {opt_size[1]} Used, {opt_size[3]} Available")
            if tmp_size:
                print(f"/tmp Size: {tmp_size[1]} Used, {tmp_size[3]} Available")
            client.close()
        except paramiko.AuthenticationException:
            print(f"Failed to connect to {node}: Invalid credentials")
        except paramiko.SSHException:
            print(f"Failed to connect to {node}: SSH connection failed")
        except paramiko.ssh_exception.NoValidConnectionsError:
            print(f"Failed to connect to {node}: No valid connections")

