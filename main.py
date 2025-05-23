import os
import xxhash
import shutil
import subprocess
import time
import pymysql
import json
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

load_dotenv()

# ==== Configuration ====
try:
    with open('config.json', 'r') as file:
        config = json.load(file)
        meta_db = config['meta_db']
        DRIVE_FOLDER_ID = config['drive_folder_id']
        SERVICE_ACCOUNT_FILE = config['service_account_file']
        directories_to_backup = config['directories']
        database_names = config['databases']
except Exception as e:
    logging.error(f"Failed to load configuration: {e}")
    raise

db_config = {
    'host': os.getenv('DB_HOSTNAME'),
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD')
}

meta_table = 'tbl_backup'
SCOPES = ['https://www.googleapis.com/auth/drive.file']


def compute_hash(path):
    logging.debug(f"Computing hash for: {path}")
    hasher = xxhash.xxh64()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_db_connection(database=None):
    return pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=database,
        autocommit=True
    )


def get_previous_hash(logical_name):
    conn = get_db_connection(meta_db)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT hash FROM {meta_table} WHERE name=%s", (logical_name,))
        row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def update_hash(logical_name, new_hash):
    logging.info(f"Updating hash for {logical_name}")
    conn = get_db_connection(meta_db)
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {meta_table}(name, hash) VALUES(%s, %s) "
            "ON DUPLICATE KEY UPDATE hash=VALUES(hash)",
            (logical_name, new_hash)
        )
    conn.close()


def get_drive_service():
    logging.info("Initializing Google Drive service...")
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build('drive', 'v3', credentials=creds)


def upload_to_drive(local_path, folder_id, service):
    filename = os.path.basename(local_path)
    logging.info(f"Uploading: {filename} to Google Drive")
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    resp = service.files().list(q=query, fields='files(id)').execute()
    files = resp.get('files', [])
    media = MediaFileUpload(local_path, resumable=True)
    if files:
        logging.info(f"File exists. Updating existing file: {filename}")
        return service.files().update(fileId=files[0]['id'], media_body=media).execute()
    else:
        logging.info(f"Creating new file on Drive: {filename}")
        file_metadata = {'name': filename, 'parents': [folder_id]}
        return service.files().create(body=file_metadata, media_body=media).execute()


def backup_directories(service):
    logging.info("Starting directory backup...")
    timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
    for directory in directories_to_backup:
        if not os.path.isdir(directory['path']):
            logging.warning(f"Directory not found: {directory['path']}")
            continue
        logical_name = f"{directory['name']}_{os.path.basename(directory['path'])}"
        zip_name = f"{logical_name}_{timestamp}.zip"
        logging.info(f"Archiving directory: {directory['path']} → {zip_name}")
        shutil.make_archive(zip_name[:-4], 'zip', directory['path'])

        h = compute_hash(zip_name)
        prev = get_previous_hash(logical_name)

        if h != prev:
            logging.info(f"Detected changes in {logical_name}, uploading...")
            upload_to_drive(zip_name, DRIVE_FOLDER_ID, service)
            update_hash(logical_name, h)
        else:
            logging.info(
                f"No changes detected in {logical_name}, skipping upload.")

        os.remove(zip_name)
        logging.debug(f"Removed temporary archive: {zip_name}")


def backup_databases(service):
    logging.info("Starting database backup...")
    timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
    for db in database_names:
        logical_name = db
        dump_file = f"{logical_name}_{timestamp}.sql"
        cmd = [
            'mysqldump',
            '--skip-dump-date',
            '--skip-comments',
            f"-u{db_config['user']}",
            f"-p{db_config['password']}",
            db
        ]

        logging.info(f"Dumping database: {db} → {dump_file}")
        try:
            with open(dump_file, 'wb') as f:
                subprocess.run(cmd, stdout=f, check=True)
        except subprocess.CalledProcessError as e:
            logging.error(f"mysqldump failed for {db}: {e}")
            continue

        h = compute_hash(dump_file)
        prev = get_previous_hash(logical_name)

        if h != prev:
            logging.info(
                f"Changes detected in DB {logical_name}, uploading...")
            upload_to_drive(dump_file, DRIVE_FOLDER_ID, service)
            update_hash(logical_name, h)
        else:
            logging.info(
                f"No changes detected in DB {logical_name}, skipping upload.")

        os.remove(dump_file)
        logging.debug(f"Removed temporary SQL dump: {dump_file}")


def main():
    logging.info("Backup process started.")
    try:
        service = get_drive_service()
        backup_directories(service)
        backup_databases(service)
        logging.info("Backup process completed successfully.")
    except Exception as e:
        logging.exception(f"Backup process failed: {e}")


if __name__ == '__main__':
    main()
