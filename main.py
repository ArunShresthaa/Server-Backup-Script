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
        excluded_tables = config.get('excluded_tables', {})
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


def get_or_create_drive_folder_by_name(name, parent_id, service):
    query = (
        f"mimeType='application/vnd.google-apps.folder' and "
        f"name='{name}' and '{parent_id}' in parents and trashed=false"
    )
    results = service.files().list(q=query, fields="files(id)").execute()
    folders = results.get('files', [])
    if folders:
        return folders[0]['id']

    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    folder = service.files().create(body=file_metadata, fields='id').execute()
    logging.info(f"Created new dated folder: {name}")
    return folder['id']


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


def backup_directories(service, folder_id):
    logging.info("Starting directory backup...")
    for directory in directories_to_backup:
        if not os.path.isdir(directory['path']):
            logging.warning(f"Directory not found: {directory['path']}")
            continue

        logical_name = f"{directory['name']}_{os.path.basename(directory['path'])}"
        zip_name = f"{logical_name}.zip"
        logging.info(f"Archiving directory: {directory['path']} → {zip_name}")
        shutil.make_archive(zip_name[:-4], 'zip', directory['path'])

        h = compute_hash(zip_name)
        prev = get_previous_hash(logical_name)

        if h != prev:
            logging.info(f"Detected changes in {logical_name}, uploading...")
            upload_to_drive(zip_name, folder_id, service)
            update_hash(logical_name, h)
        else:
            logging.info(
                f"No changes detected in {logical_name}, skipping upload.")

        os.remove(zip_name)
        logging.debug(f"Removed temporary archive: {zip_name}")


def backup_databases(service, folder_id):
    logging.info("Starting database backup...")
    for db in database_names:
        dump_file = f"{db}.sql"
        cmd = [
            'mysqldump',
            '--skip-dump-date',
            '--skip-comments',
            f"-u{db_config['user']}",
            f"-p{db_config['password']}",
            db
        ]

        # Process tables with no data (structure only)
        tables_no_data = excluded_tables.get(db, [])
        if tables_no_data:
            logging.info(
                f"Excluding data for tables in {db}: {', '.join(tables_no_data)}")
            # First create a full schema dump with all tables
            for table in tables_no_data:
                # Add individual no-data tables with specific options
                cmd.extend([
                    '--ignore-table-data',
                    f'{db}.{table}'
                ])

        logging.info(f"Dumping database: {db} → {dump_file}")
        try:
            with open(dump_file, 'wb') as f:
                subprocess.run(cmd, stdout=f, check=True)
        except subprocess.CalledProcessError as e:
            logging.error(f"mysqldump failed for {db}: {e}")
            continue

        h = compute_hash(dump_file)
        prev = get_previous_hash(db)

        if h != prev:
            logging.info(f"Changes detected in DB {db}, uploading...")
            upload_to_drive(dump_file, folder_id, service)
            update_hash(db, h)
        else:
            logging.info(f"No changes detected in DB {db}, skipping upload.")

        os.remove(dump_file)
        logging.debug(f"Removed temporary SQL dump: {dump_file}")


def main():
    logging.info("Backup process started.")
    try:
        service = get_drive_service()
        date_str = time.strftime('%Y-%m-%d_%H-%M-%S')
        dated_folder_id = get_or_create_drive_folder_by_name(
            date_str, DRIVE_FOLDER_ID, service)

        backup_directories(service, dated_folder_id)
        backup_databases(service, dated_folder_id)

        logging.info("Backup process completed successfully.")
    except Exception as e:
        logging.exception(f"Backup process failed: {e}")


if __name__ == '__main__':
    main()
