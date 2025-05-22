import os
import hashlib
import shutil
import subprocess
import time
import pymysql
import msal
import requests

# ==== Configuration ====
# Folders to zip
directories_to_backup = [
    '"C:\Users\ArunShrestha\Desktop\khwopa source code"'
    # '/path/to/folder2',
]
# MySQL settings
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
}
# Databases to dump
database_names = ['test']
# Backup metadata table
meta_db = 'backup'
meta_table = 'tbl_backup'
# OneDrive / Microsoft Graph settings
TENANT_ID = 'your_tenant_id'
CLIENT_ID = 'your_client_id'
CLIENT_SECRET = 'your_client_secret'
SCOPES = ['https://graph.microsoft.com/.default']
ONEDRIVE_FOLDER = '/backups'

# ==== Helper Functions ====


def compute_hash(path):
    """Compute SHA256 hash of a file."""
    sha = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def get_db_connection(database=None):
    conn = pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=database,
        autocommit=True
    )
    return conn


def get_previous_hash(name):
    conn = get_db_connection(meta_db)
    with conn.cursor() as cur:
        cur.execute(f"SELECT hash FROM {meta_table} WHERE name=%s", (name,))
        row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def update_hash(name, new_hash):
    conn = get_db_connection(meta_db)
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {meta_table}(name, hash) VALUES(%s, %s) "
            "ON DUPLICATE KEY UPDATE hash=VALUES(hash)",
            (name, new_hash)
        )
    conn.close()


def acquire_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f'https://login.microsoftonline.com/{TENANT_ID}',
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_silent(SCOPES, account=None)
    if not result:
        result = app.acquire_token_for_client(scopes=SCOPES)
    if 'access_token' in result:
        return result['access_token']
    else:
        raise Exception(
            f"Could not obtain access token: {result.get('error_description')}")


def upload_file_to_onedrive(local_path, remote_folder, token):
    filename = os.path.basename(local_path)
    endpoint = (
        f"https://graph.microsoft.com/v1.0/me/drive/root:{remote_folder}/{filename}:/content"
    )
    headers = {'Authorization': f'Bearer {token}'}
    with open(local_path, 'rb') as f:
        resp = requests.put(endpoint, headers=headers, data=f)
    resp.raise_for_status()
    return resp.json()

# ==== Backup Tasks ====


def backup_directories():
    token = acquire_token()
    for path in directories_to_backup:
        if not os.path.isdir(path):
            continue
        name = os.path.basename(path)
        zip_name = f"{name}_{time.strftime('%Y%m%d')}.zip"
        shutil.make_archive(name, 'zip', path)
        h = compute_hash(zip_name)
        prev_h = get_previous_hash(zip_name)
        if h != prev_h:
            upload_file_to_onedrive(zip_name, ONEDRIVE_FOLDER, token)
            update_hash(zip_name, h)
        os.remove(zip_name)


def backup_databases():
    token = acquire_token()
    for db in database_names:
        dump_file = f"{db}_{time.strftime('%Y%m%d')}.sql"
        cmd = [
            'mysqldump',
            f"-u{db_config['user']}",
            f"-p{db_config['password']}",
            db
        ]
        with open(dump_file, 'wb') as f:
            subprocess.run(cmd, stdout=f, check=True)
        h = compute_hash(dump_file)
        prev_h = get_previous_hash(dump_file)
        if h != prev_h:
            upload_file_to_onedrive(dump_file, ONEDRIVE_FOLDER, token)
            update_hash(dump_file, h)
        os.remove(dump_file)


def main():
    backup_directories()
    backup_databases()


if __name__ == '__main__':
    main()
