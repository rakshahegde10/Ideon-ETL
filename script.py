import boto3
import os
import tarfile
import glob

# create an S3 client object
s3 = boto3.client('s3')

# define the bucket name and file path
bucket_name = 'bulk-data.vericred.com'
file_path = 'networks/nyu_capstone/providers.tar.gz'

# get the path to the user's desktop
desktop_path = os.path.expanduser("~/Desktop")

# create the data_set directory on the desktop
data_set_path = os.path.join(desktop_path, "Project")
os.makedirs(data_set_path, exist_ok=True)

# create the current subdirectory inside data_set
current_path = os.path.join(data_set_path, "current")
os.makedirs(current_path, exist_ok=True)

destination_path = os.path.join(data_set_path, 'providers.tar.gz')

# download the file from S3 to the local destination path
s3.download_file(bucket_name, file_path, destination_path)

#Extracting the file
with tarfile.open(f'{data_set_path}/providers.tar.gz', 'r:gz') as tar:
    tar.extractall(f'{data_set_path}/')


#Moving the file
for file_path in glob.glob(os.path.join(data_set_path, '*.jsonl')):
    file_name = os.path.basename(file_path)
    new_file_path = os.path.join(current_path, file_name)
    os.rename(file_path, new_file_path)