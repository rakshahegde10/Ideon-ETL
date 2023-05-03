#Please join this with the other script file in the repository


source_bucket_name = 'nyu-capstone-2023'
destination_bucket_name = 'nyu-capstone-2023'

# specify the source and destination folder paths
source_folder_path = 'data/current/'
destination_folder_path = 'data/archive/'

# check if the source folder is not empty
response = s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_folder_path)
if 'Contents' in response:
    # list all JSONL files in the source folder
    response = s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_folder_path, Delimiter='/')
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.jsonl'):
            # construct the source and destination file paths
            source_key = obj['Key']
            destination_key = destination_folder_path + source_key.split('/')[-1]

            # copy the file to the destination folder
            s3.copy_object(Bucket=destination_bucket_name, CopySource={'Bucket': source_bucket_name, 'Key': source_key}, Key=destination_key)

            # delete the file from the source folder
            s3.delete_object(Bucket=source_bucket_name, Key=source_key)
        else:
            print("The current folder is empty.")


# Set the name of the S3 bucket
bucket_name = 'nyu-capstone-2023'

# Set the name of the S3 folder to upload the files to
s3_folder = 'data/current'

for filename in os.listdir(current_path):
    if filename.endswith('.jsonl'):
        # Set the local file path
        local_file = os.path.join(current_path, filename)
        
        # Set the S3 object key
        s3_key = f'{s3_folder}/{filename}'
        
        # Upload the file to S3
        with open(local_file, 'rb') as f:
            s3.upload_fileobj(f, bucket_name, s3_key)


#Deleting the file from local directory
files = glob.glob(f'{data_set_path}/*.tar.gz')
for file in files:
    os.remove(file)
files = glob.glob(f'{current_path}/*.jsonl')
for file in files: