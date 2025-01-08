import boto3
import os
from botocore.exceptions import ClientError

class SimpleStorageService:
    """
    Class responsible for handling  S3 Buckets
    """
    def __init__(self, region='ap-south-1'):
        self.region = region
        self.s3_client = None

    ## Function 1 : Creating S3 bucket using Function
    def create_bucket(self, bucket_name=None):
        """
        Function responsible for creating S3 bucket  in a specified region
        :param bucket_name:  Name of the S3 bucket -> default : None
        :param region:  Name of the region -> default : None
        :return: True if bucket is created else false
        """
        region = self.region

        ## PHASE 1 : Creating Bucket
        try:
            if region:
                s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
            else:
                s3_client = boto3.client('s3')
                s3_client.create_bucket(Bucket=bucket_name)
            self.s3_client = s3_client
        except Exception as e:
            print(f"ERROR (Create Bucket): {str(e)}")
            return False
        return True



    def list_buckets(self):
        """
        Function Responsible ofr listing all buckets
        :return: List
        """
        region = self.region

        try:
            s3_client = boto3.client('s3', region_name=region)
            self.s3_client = s3_client
            response = s3_client.list_buckets()
            print("List of S3 buckets . . . ")
            for idx, bucket in enumerate(response['Buckets']):
                print(f"{idx+1}. {bucket['Name']}")

        except Exception as e:
            print(f"ERROR (List Buckets): {str(e)}")


    def upload_object(self, file_name, bucket, object_name=None ):
        """
        Function Responsible for uploading files to S3 Bucket
        :param file_name:  Takes File name
        :param bucket: Takes in bucket name to upload file to
        :param object_name: Either pass in the S3 name or cosider file name
        :return: Bool -> True or Flase based on the entry made
        """
        ## Phase 1 : Creating path
        if object_name is None:
            object_name = os.path.basename(file_name)

        ## Phase 2 Creatigng Client
        s3_client = self.s3_client
        try:
            reponse = s3_client.upload_file(file_name, bucket, object_name)
            if reponse:
                print(f"{file_name} file uploaded to {bucket} buket successfully!")
        except ClientError as e:
            print(f"Client Error: {str(e)}")
            return False
        return True





### DRIVER CODE  ###
#  Phase 1 : Creating Bucket
#create_bucket(bucket_name="test-bucket-mumbai-s3", region = 'ap-south-1' )

# Phase 2 : List Bucket
boto_obj = SimpleStorageService()
boto_obj.list_buckets()

# Phase 3 : Adding Objects
booldata = boto_obj.upload_object(file_name=r"D:\ORIM\CLOUD\AWS_BoTo3\files\Anya Polytech &_C.xlsx", bucket="test-bucket-mumbai-s3")
if booldata:
    print("File uploaded")
else:
    print("File Not uploaded")