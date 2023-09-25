import os
import glob

directory = '/data01/cdr/IPDR/flag/'  # Replace with your desired directory

# Get a list of all files in the directory
files = glob.glob(os.path.join(directory, '*'))

# Sort files based on their modification time in ascending order
files = sorted(files, key=os.path.getmtime)

# Select the oldest 100 files
files_to_delete = files[:100]

# Delete the selected files
for file_path in files_to_delete:
    os.remove(file_path)
    print(f"Deleted: {file_path}")