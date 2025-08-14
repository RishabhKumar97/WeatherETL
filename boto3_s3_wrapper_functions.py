import boto3
import os
from pprint import pprint
import uuid
import re
    
def generate_bucket_name(bucket_prefix):
    return bucket_prefix+"-"+str(uuid.uuid4())

def create_bucket_if_prefix_not_exists(bucket_prefix, s3_client):
    session = boto3.session.Session()
    current_region = session.region_name
    if(not check_bucket_prefix_exists(bucket_prefix, s3_client)):
        bucket_response = s3_client.create_bucket(
            Bucket=generate_bucket_name(bucket_prefix),
            CreateBucketConfiguration={
            'LocationConstraint': current_region})
        return (bucket_prefix, current_region)
    else:
        print(f"Bucket with the prefix {bucket_prefix} already exists")
        return None

def get_bucket_name(bucket_prefix, s3_client):
     for info in s3_client.list_buckets()['Buckets']:
        if re.search(bucket_prefix.lower(), info['Name']):
            return info['Name']

def check_bucket_prefix_exists(bucket_prefix, s3_client):
    for info in s3_client.list_buckets()['Buckets']:
        if re.search(bucket_prefix.lower(), info['Name']):
            return True
        else:
            return False

def delete_from_bucket(bucket_name, keys_to_delete):
    if(check_bucket_prefix_exists(bucket_name, s3_client)):
        for key in keys_to_delete:
            s3_client.delete_object(Bucket= bucket_name, Key= key)
    else:
        print(f"{bucket_name} doesn't exists")
        return False

def delete_bucket(bucket_name, s3_client):
    if(check_bucket_prefix_exists(bucket_name, s3_client)):
        s3_client.delete_bucket(Bucket= bucket_name)

def get_file_names_in_dir(dir_name):
    files_list= []
    for d, sd, files in os.walk(dir_name):
        for f in files:
            file_path= os.path.join(d, f)
            files_list.append(file_path.replace(os.sep, '/'))
    return files_list

def load_files_to_s3(bucket_name, s3_client, files_path_list):
    for file in files_path_list:
        name= file.split(os.sep)[-1]
        try:
            print(f"Uploading file {name}")
            s3_client.upload_file(file, bucket_name, file)
            print(f" File {name} uploaded successfully")
        except Exception as e:
            print(f"Error occured while uploading the file {name}")
            print(e)
            continue

def list_objects(bucket_name, s3_client):
    resp = s3_client.list_objects(Bucket= bucket_name)
    output_objs = []
    for obj in resp['Contents']:
        output_objs.append(obj['Key'])
    return output_objs

if __name__ == "__main__":
    s3_client = boto3.client("s3")
    bucket_prefix = "weather-data-raw"
    if(not check_bucket_prefix_exists(bucket_prefix, s3_client)):
        create_bucket_if_prefix_not_exists(bucket_prefix, s3_client)        
    bucketName = get_bucket_name(bucket_prefix, s3_client)
    print(f"Bucket name is {bucketName}")

    #gather file paths in the directory to upload to s3
    # file_paths = get_file_names_in_dir('weather_cron_files')
    # load_files_to_s3(bucketName, s3_client, file_paths)
    # remove = list_objects("bucketName", s3_client)
    # delete_from_bucket("bucketName", remove)

    
