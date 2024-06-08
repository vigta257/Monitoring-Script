import os
import pandas as pd
from datetime import datetime

# Define the folders to monitor
folders = [
    '/home/victor/config_dir/facility_1',
    '/home/victor/config_dir/facility_2',
    '/home/victor/config_dir/facility_3',
    '/home/victor/config_dir/facility_4'
]

# Define the output directory and CSV files
output_dir = '/home/victor/config_dir'
output_file = os.path.join(output_dir, 'Monitoring.csv')
log_file = os.path.join(output_dir, 'log.csv')

# Load existing data if the file exists
if os.path.exists(output_file):
    df = pd.read_csv(output_file)
else:
    df = pd.DataFrame(columns=['Date', 'Folder', 'File Name', 'File Size (MB)'])

# Initialize a set to store known files
known_files = set(df['File Name'])

# List to collect new file details
new_entries = []

# Function to get file details
def get_file_details(file_path):
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    return {
        'Date': datetime.now().strftime('%Y-%m-%d'),  # Only the date
        'Folder': os.path.basename(os.path.dirname(file_path)),  # Parent directory name
        'File Name': os.path.basename(file_path),
        'File Size (MB)': size_mb
    }

# Function to write log messages
def write_log(message):
    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = {'Timestamp': log_time, 'Message': message}
    if os.path.exists(log_file):
        log_df = pd.read_csv(log_file)
    else:
        log_df = pd.DataFrame(columns=['Timestamp', 'Message'])
    log_df = pd.concat([log_df, pd.DataFrame([log_entry])], ignore_index=True)
    log_df.to_csv(log_file, index=False)

# Check for new files in each folder
for folder in folders:
    for root, dirs, files in os.walk(folder):
        for file in files:
            file_path = os.path.join(root, file)
            if file not in known_files:
                file_details = get_file_details(file_path)
                new_entries.append(file_details)
                known_files.add(file)

# Create a DataFrame from the new entries
if new_entries:
    new_entries_df = pd.DataFrame(new_entries)
    # Concatenate the existing DataFrame with the new entries DataFrame
    df = pd.concat([df, new_entries_df], ignore_index=True)

# Write updated data to the CSV file
df.to_csv(output_file, index=False)

# Write log message
write_log("Monitoring complete. Data written to Monitoring.csv")

# Print the log message (optional)
#print("Monitoring complete. Data written to Monitoring.csv")
