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