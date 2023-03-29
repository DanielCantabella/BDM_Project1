import paramiko


def connect_ssh(host, user, pwd):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=pwd)
    return client


def close_ssh(client):
    client.close()


def sftp_get_file(client, remote_path, local_path):
    sftp = client.open_sftp()
    sftp.get(remote_path, local_path)
    sftp.close()

def sftp_put_file(client, remote_file_path, local_file_path):
    sftp = client.open_sftp()
    sftp.put(local_file_path, remote_file_path)
    sftp.close()


