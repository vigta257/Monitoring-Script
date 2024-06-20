import os
import psycopg2
from datetime import datetime

# Define the folders to monitor
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
    'host': '192.168.0.110',
    'port': 5432
}

# Log file path
log_file_path = '/home/victor/config_dir/log_file.log'  # Specify the path to your log file

# Function to write log messages to a file
def write_log(message):
    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"[{log_time}] {message}\n")

# Connect to the database
try:
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    write_log("Connected to the database successfully.")
except Exception as e:
    write_log(f"Failed to connect to the database: {str(e)}")
    exit()

# Create table if it doesn't exist, with 'read_time' column after 'date'
cur.execute("""
    CREATE TABLE IF NOT EXISTS folder_monitoring (
        id SERIAL PRIMARY KEY,
        date DATE,
        read_time TIME,
        folder VARCHAR(255),
        file_name VARCHAR(255),
        file_size_mb FLOAT
    )
""")
conn.commit()

# Function to get file details
def get_file_details(file_path):
    try:
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        creation_time = datetime.fromtimestamp(os.path.getctime(file_path))  # Get file creation time
        file_date = creation_time.strftime('%Y-%m-%d')  # Extract the date part
        file_time = creation_time.strftime('%H:%M:%S')  # Extract the time part
        return {
            'date': file_date,
            'read_time': file_time,
            'folder': os.path.basename(os.path.dirname(file_path)),
            'file_name': os.path.basename(file_path),
            'file_size_mb': size_mb
        }
    except Exception as e:
        write_log(f"Error getting details for {file_path}: {str(e)}")
        return None

# Check for new files in each folder
for folder in folders:
    for root, dirs, files in os.walk(folder):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                cur.execute("SELECT 1 FROM folder_monitoring WHERE file_name = %s", (file,))
                if cur.fetchone() is None:
                    file_details = get_file_details(file_path)
                    if file_details:
                        cur.execute("""
                            INSERT INTO folder_monitoring (date, read_time, folder, file_name, file_size_mb)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            file_details['date'],
                            file_details['read_time'],
                            file_details['folder'],
                            file_details['file_name'],
                            file_details['file_size_mb']
                        ))
                        conn.commit()
                        write_log(f"Inserted: {file_details}")
            except Exception as e:
                write_log(f"Database error for file {file}: {str(e)}")

# Close the database connection
cur.close()
conn.close()

# Write log message
write_log("Monitoring complete. Data written to database")
