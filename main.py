import os
import hashlib
import shutil
import subprocess
import time
import pymysql
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ==== Configuration ====
# Folders to zip
directories_to_backup = [
    '/path/to/folder1',
    '/path/to/folder2',
]
# MySQL settings
db_config = {
    'host': 'localhost',
    'user': 'username',
    'password': 'password',
}
# Databases to dump
database_names = ['db1', 'db2']
# Backup metadata table
meta_db = 'backup'
meta_table = 'tbl_backup'
# Google Drive settings
SERVICE_ACCOUNT_FILE = '/path/to/service-account.json'
DRIVE_FOLDER_ID = 'your_drive_folder_id'
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# ==== Helper Functions ====


def compute_hash(path):
    """Compute SHA256 hash of a file."""
    sha = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def get_db_connection(database=None):
    return pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=database,
        autocommit=True
    )


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


def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build('drive', 'v3', credentials=creds)


def upload_to_drive(local_path, folder_id, service):
    """Uploads or updates a file in Google Drive folder."""
    filename = os.path.basename(local_path)
    # Check if file exists
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    resp = service.files().list(q=query, fields='files(id)').execute()
    files = resp.get('files', [])
    media = MediaFileUpload(local_path, resumable=True)
    if files:
        file_id = files[0]['id']
        return service.files().update(fileId=file_id, media_body=media).execute()
    else:
        file_metadata = {'name': filename, 'parents': [folder_id]}
        return service.files().create(body=file_metadata, media_body=media).execute()

# ==== Backup Tasks ====


def backup_directories(service):
    for path in directories_to_backup:
        if not os.path.isdir(path):
            continue
        name = os.path.basename(path)
        zip_name = f"{name}_{time.strftime('%Y%m%d')}.zip"
        shutil.make_archive(name, 'zip', path)
        h = compute_hash(zip_name)
        prev = get_previous_hash(zip_name)
        if h != prev:
            upload_to_drive(zip_name, DRIVE_FOLDER_ID, service)
            update_hash(zip_name, h)
        os.remove(zip_name)


def backup_databases(service):
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
        prev = get_previous_hash(dump_file)
        if h != prev:
            upload_to_drive(dump_file, DRIVE_FOLDER_ID, service)
            update_hash(dump_file, h)
        os.remove(dump_file)


def main():
    service = get_drive_service()
    backup_directories(service)
    backup_databases(service)


if __name__ == '__main__':
    main()
