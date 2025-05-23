# Server Backup Script

A Python-based automated backup solution for backing up directories and MySQL databases to Google Drive with change detection.

## Overview

This script provides an automated solution for backing up specified directories and MySQL databases to Google Drive. It uses a hash-based approach to detect changes, ensuring that only modified content is uploaded, which saves bandwidth and storage space.

## Features

- **Directory Backup**: Compresses and backs up specified directories
- **Database Backup**: Creates SQL dumps of specified MySQL databases
- **Change Detection**: Uses xxHash algorithm to detect changes in files/databases
- **Google Drive Integration**: Automatically uploads backups to Google Drive
- **Smart Updates**: Only uploads files that have changed since the last backup
- **Detailed Logging**: Provides comprehensive logging of all operations

## Prerequisites

- Python 3.6+
- MySQL/MariaDB server
- Google Cloud Platform account with Drive API enabled
- MySQL command-line tools (mysqldump)

## Installation

1. Clone this repository or download the scripts to your server
2. Install required Python packages:

```bash
pip install -r requirements.txt
```

3. Set up a Google Cloud Platform project:
   - Create a project in the [Google Cloud Console](https://console.cloud.google.com/)
   - Enable the Google Drive API
   - Create a service account with appropriate permissions
   - Download the service account credentials as JSON

4. Create a `.env` file with your database credentials:

```
DB_HOSTNAME=your_db_host
DB_USERNAME=your_db_username
DB_PASSWORD=your_db_password
```

5. Create the metadata database and table using the provided SQL script:

```bash
mysql -u your_username -p < backup_log.sql
```

## Configuration

Make the `config.json` file to configure your backup settings:

```json
{
    "drive_folder_id": "your_google_drive_folder_id",
    "meta_db": "server_backup",
    "service_account_file": "service-account.json",
    "directories": [
        {
            "name": "Logical Name",
            "path": "/path/to/directory"
        }
    ],
    "databases": [
        "database1",
        "database2"
    ]
}
```

### Configuration Options

- `drive_folder_id`: The ID of the Google Drive folder where backups will be stored
- `meta_db`: The name of the database where backup metadata is stored
- `service_account_file`: Path to your Google service account credentials file
- `directories`: List of directories to back up, each with a logical name and file path
- `databases`: List of database names to back up

## Usage

Run the script manually:

```bash
python main.py
```

For automated backups, set up a cron job (Linux) or Task Scheduler (Windows) to run the script at desired intervals.

Example cron job (daily at Midnight):

```
0 0 * * * cd /home/chatbot/Server-Backup-Script && /home/chatbot/miniconda3/envs/server_backup/bin/python main.py >> backup.log 2>&1
```

## How It Works

1. The script loads configuration from `config.json` and environment variables
2. For each configured directory:
   - Creates a ZIP archive of the directory
   - Computes a hash of the archive to detect changes
   - Uploads to Google Drive if changes are detected
   - Updates the hash in the metadata database
   - Removes the temporary ZIP file

3. For each configured database:
   - Creates an SQL dump using mysqldump
   - Computes a hash of the dump to detect changes
   - Uploads to Google Drive if changes are detected
   - Updates the hash in the metadata database
   - Removes the temporary SQL file

## Logging

The script logs all operations to both the console and a `backup.log` file, providing details about each step in the backup process. This includes:

- Backup initiation and completion
- Archive creation
- Hash computation
- Change detection
- Upload status
- Error information

## Error Handling

The script includes robust error handling to manage common issues:

- Missing directories are logged and skipped
- Database dump failures are logged and processing continues with the next database
- Configuration errors are reported with detailed messages
- All exceptions are caught, logged, and reported

## Security Considerations

- Store your `.env` file securely and restrict access
- Keep your Google service account credentials secure
- Consider using encrypted backups for sensitive data
- Ensure proper permissions on the script and configuration files

## Troubleshooting

- Check the `backup.log` file for detailed error messages
- Verify that all paths in `config.json` are correct and accessible
- Ensure database credentials in `.env` have appropriate permissions
- Confirm that the Google Drive API is enabled and the service account has proper access