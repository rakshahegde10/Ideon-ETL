import boto3
import os
import tarfile
import glob
import logging
import datetime

os.environ['IDEON_BUCKET_NAME'] = 'bulk-data.vericred.com'
os.environ['IDEON_FILE_PATH'] = 'networks/nyu_capstone/providers.tar.gz'
os.environ['TAR_FILE'] = 'providers.tar.gz'
os.environ['NYU_BUCKET_NAME'] = 'nyu-capstone-2023'
os.environ['NYU_CURRENT_PATH'] = 'data/current/'
os.environ['NYU_ARCHIVE_PATH'] = 'data/archive/'

# create logger
logger = logging.getLogger("Ideon ETL")
logger.setLevel(logging.INFO)
# create console handler and set level to info
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

# create an S3 client object
s3 = boto3.client('s3')

# get the path to the user's desktop
desktop_path = os.path.expanduser("~/Desktop")

# create the Project directory on the local desktop
local_directory = os.path.join(desktop_path, "Project")
os.makedirs(local_directory, exist_ok=True)

# create the Current sub-directory inside Project directory
current_path = os.path.join(local_directory, "current")
os.makedirs(current_path, exist_ok=True)

local_destination_path = os.path.join(local_directory, os.environ['TAR_FILE'])

logger.info('Download the file from S3 to the local destination path')
try:
  #download tar file from Ideon S3 bucket
  s3.download_file(os.environ['IDEON_BUCKET_NAME'], os.environ['IDEON_FILE_PATH'], local_destination_path)
except:
  logger.error("An exception occurred while downloading file from Ideon S3 Bucket")


#Un-compress the tar file
logger.info(f'Extracting the latest S3 file {local_directory}/providers.tar.gz')
try:
  with tarfile.open(f'{local_directory}/providers.tar.gz', 'r:gz') as tar:
    tar.extractall(f'{local_directory}/')
except:
  logger.error("An exception occurred while extracting from tar file")

logger.info('Moving the current S3 file to archive')
for file_path in glob.glob(os.path.join(local_directory, '*.jsonl')):
    file_name = os.path.basename(file_path)
    new_file_path = os.path.join(current_path, file_name)
    os.rename(file_path, new_file_path)


# check if the source folder is not empty
response = s3.list_objects_v2(Bucket=os.environ['NYU_BUCKET_NAME'], Prefix=os.environ['NYU_CURRENT_PATH'])
now = datetime.datetime.now()
date_string = now.strftime("%Y%m%d")
if 'Contents' in response:
    # list all JSONL files in the source folder
    response = s3.list_objects_v2(Bucket=os.environ['NYU_BUCKET_NAME'], Prefix=os.environ['NYU_CURRENT_PATH'], Delimiter='/')
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.jsonl'):
            # construct the source and destination file paths
            source_key = obj['Key']
            source_file_name = source_key.split('/')[-1]
            destination_file_name = date_string + '.jsonl'
            destination_key = os.environ['NYU_ARCHIVE_PATH'] + destination_file_name

            logger.info(f'Copying the old file from current folder to archive folder in NYU S3 bucket')
            try:
                s3.copy_object(Bucket=os.environ['NYU_BUCKET_NAME'], CopySource={'Bucket': os.environ['NYU_BUCKET_NAME'], 'Key': source_key}, Key=destination_key)
            except:
                logger.error("An exception occurred while copying file to archive in S3")

            # delete the file from the source folder
            logger.info(f'Deleting the old file from current folder in NYU S3 bucket')
            try:
                s3.delete_object(Bucket=os.environ['NYU_BUCKET_NAME'], Key=source_key)
            except:
                logger.error("An exception occurred while deleting file in current folder in S3")

#upload most recent file to NYU S3 bucket
for filename in os.listdir(current_path):
    if filename.endswith('.jsonl'):
        # Set the local file path
        local_file = os.path.join(current_path, filename)
        # Set the S3 object key
        s3_key = f'data/current/{filename}'
        # Upload the file to S3
        logger.info(f'Upload new file to current folder of NYU S3 bucket')
        with open(local_file, 'rb') as f:
            try:
                s3.upload_fileobj(f, os.environ['NYU_BUCKET_NAME'], s3_key)
            except:
                logger.error("An exception occurred while deleting file in current folder in S3")


logger.info('Deleting the local directories')
files = glob.glob(f'{local_directory}/*.tar.gz')
for file in files:
    os.remove(file)
files = glob.glob(f'{current_path}/*.jsonl')
for file in files:
    os.remove(file)
os.rmdir(current_path)
os.rmdir(local_directory)

logger.info('Script End')
