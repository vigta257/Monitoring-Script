import os
import psycopg2
from datetime import datetime

# Define the folders to monitor with full paths
folders = [
    '/home/victor/config_dir/facility_1',
    '/home/victor/config_dir/facility_2',
    '/home/victor/config_dir/facility_3',
    '/home/victor/config_dir/facility_4'
]

# Database connection parameters
db_params = {
    'dbname': 'monitoring',
    'user': 'postgres',
    'password': 'postgres',
    'host': '192.168.0.190',
    'port': 5432
}

# Log file path
log_file_path = '/home/victor/config_dir/log_file.log'

# Function to write log messages to a file
def write_log(message):
    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"[{log_time}] {message}\n")

# Function to connect to the database
def connect_db(params):
    try:
        conn = psycopg2.connect(**params)
        write_log("Connected to the database successfully.")
        return conn
    except Exception as e:
        write_log(f"Failed to connect to the database: {str(e)}")
        exit()

# Function to check if the folder_monitoring table exists
def check_table_exists(cur):
    try:
        cur.execute("SELECT to_regclass('public.folder_monitoring')")
        table_exists = cur.fetchone()[0] is not None
        if not table_exists:
            write_log("The folder_monitoring table does not exist.")
            exit()
        else:
            write_log("The folder_monitoring table exists.")
    except Exception as e:
        write_log(f"Failed to check if table exists: {str(e)}")
        exit()

# Function to get file details
def get_file_details(file_path):
    try:
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
        file_date = creation_time.strftime('%Y-%m-%d')
        file_time = creation_time.strftime('%H:%M:%S')
        folder_name = os.path.basename(os.path.dirname(file_path))
        return {
            'date': file_date,
            'read_time': file_time,
            'folder': folder_name,
            'file_name': os.path.basename(file_path),
            'file_size_mb': size_mb,
            'creation_time': creation_time
        }
    except Exception as e:
        write_log(f"Error getting details for {file_path}: {str(e)}")
        return None

# Function to collect all file details from the folders
def collect_file_details(folders):
    all_file_details = []
    for folder in folders:
        for root, dirs, files in os.walk(folder):
            for file in files:
                file_path = os.path.join(root, file)
                file_details = get_file_details(file_path)
                if file_details:
                    all_file_details.append(file_details)
    return all_file_details

# Function to insert file details into the database
def insert_file_details(cur, file_details):
    for details in file_details:
        try:
            cur.execute("SELECT file_size_mb FROM folder_monitoring WHERE file_name = %s AND folder = %s", 
                        (details['file_name'], details['folder']))
            result = cur.fetchone()
            if result is None:
                cur.execute("""
                    INSERT INTO folder_monitoring (date, read_time, folder, file_name, file_size_mb)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    details['date'],
                    details['read_time'],
                    details['folder'],
                    details['file_name'],
                    details['file_size_mb']
                ))
                cur.connection.commit()
                write_log(f"Inserted: {details}")
            else:
                existing_size_mb = result[0]
                new_size_mb = details['file_size_mb']
                if new_size_mb != existing_size_mb:
                    cur.execute("""
                        UPDATE folder_monitoring
                        SET file_size_mb = %s, date = %s, read_time = %s
                        WHERE file_name = %s AND folder = %s
                    """, (
                        new_size_mb,
                        details['date'],
                        details['read_time'],
                        details['file_name'],
                        details['folder']
                    ))
                    cur.connection.commit()
                    write_log(f"Updated: {details['file_name']} with new size {new_size_mb} MB")
        except Exception as e:
            write_log(f"Database error for file {details['file_name']}: {str(e)}")

# Main function
def main():
    conn = connect_db(db_params)
    cur = conn.cursor()
    check_table_exists(cur)
    
    all_file_details = collect_file_details(folders)
    sorted_file_details = sorted(all_file_details, key=lambda x: x['creation_time'])
    
    insert_file_details(cur, sorted_file_details)
    
    cur.close()
    conn.close()
    write_log("Monitoring complete. Data written to database")

if __name__ == "__main__":
    main()
