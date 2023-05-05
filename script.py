import boto3
import os
import tarfile
import glob
import datetime

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
try:
  s3.download_file(bucket_name, file_path, destination_path)
except:
  print("An exception occurred while downloading file from Ideon S3 Bucket")


#Extracting the file
try:
  with tarfile.open(f'{data_set_path}/providers.tar.gz', 'r:gz') as tar:
    tar.extractall(f'{data_set_path}/')
except:
  print("An exception occurred while extracting from tar file")

#Moving the file
for file_path in glob.glob(os.path.join(data_set_path, '*.jsonl')):
    file_name = os.path.basename(file_path)
    new_file_path = os.path.join(current_path, file_name)
    os.rename(file_path, new_file_path)

source_bucket_name = 'nyu-capstone-2023'
destination_bucket_name = 'nyu-capstone-2023'

# specify the source and destination folder paths
source_folder_path = 'data/current/'
destination_folder_path = 'data/archive/'

# check if the source folder is not empty
response = s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_folder_path)
now = datetime.datetime.now()
date_string = now.strftime("%Y%m%d")
if 'Contents' in response:
    # list all JSONL files in the source folder
    response = s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_folder_path, Delimiter='/')
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.jsonl'):
            # construct the source and destination file paths
            source_key = obj['Key']
            source_file_name = source_key.split('/')[-1]
            destination_file_name = date_string + '.jsonl'
            destination_key = destination_folder_path + destination_file_name

            # copy the file to the destination folder
            try:
                s3.copy_object(Bucket=destination_bucket_name, CopySource={'Bucket': source_bucket_name, 'Key': source_key}, Key=destination_key)
            except:
                print("An exception occurred while copying file to archive in s3")

            # delete the file from the source folder
            try:
                s3.delete_object(Bucket=source_bucket_name, Key=source_key)
            except:
                print("An exception occurred while deleting file in current folder in s3")


# Set the name of the S3 bucket
bucket_name = 'nyu-capstone-2023'

# Set the name of the S3 folder to upload the files to
s3_folder = 'data/current'

print("Script is running....")

for filename in os.listdir(current_path):
    if filename.endswith('.jsonl'):
        # Set the local file path
        local_file = os.path.join(current_path, filename)
        # Set the S3 object key
        s3_key = f'{s3_folder}/{filename}'
        # Upload the file to S3
        with open(local_file, 'rb') as f:
            try:
                s3.upload_fileobj(f, bucket_name, s3_key)
            except:
                print("An exception occurred while deleting file in current folder in s3")

print("Script End")

#Deleting the file from local directory
files = glob.glob(f'{data_set_path}/*.tar.gz')
for file in files:
    os.remove(file)
files = glob.glob(f'{current_path}/*.jsonl')
for file in files:
    os.remove(file)
