import boto3
import os
import json
from botocore.exceptions import ClientError

class SimpleStorageService:
    """
    Class responsible for handling  S3 Buckets
    """
    def __init__(self, region='ap-south-1'):
        self.s3_client =  boto3.client('s3', region_name=region)

    ## Function 1 : Creating S3 bucket using Function
    def create_bucket(self, bucket_name=None, region=None):
        """
        Function responsible for creating S3 bucket  in a specified region
        :param bucket_name:  Name of the S3 bucket -> default : None
        :param region:  Name of the region -> default : None
        :return: True if bucket is created else false
        """
        ## PHASE 1 : Creating Bucket
        try:
            if region:
                location = {'LocationConstraint': region}
                self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
            else:
                self.s3_client.create_bucket(Bucket=bucket_name)
        except Exception as e:
            print(f"ERROR (Create Bucket): {str(e)}")
            return False
        return True



    def list_buckets(self):
        """
        Function Responsible ofr listing all buckets
        :return: List
        """
        try:
            s3_client = self.s3_client
            response = s3_client.list_buckets()
            print("List of S3 buckets . . . ")
            for idx, bucket in enumerate(response['Buckets']):
                print(f"{idx+1}. {bucket['Name']}")

        except ClientError as e:
            print(f"Client Error: {str(e)}")
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
            response = s3_client.upload_file(file_name, bucket, object_name)
            if response:
                print(f"{file_name} file uploaded to {bucket} buket successfully!")
        except ClientError as e:
            print(f"Client Error: {str(e)}")
            return False
        except Exception as e:
            print(f"ERROR : (upload File){str(e)}")
            return False
        return True


    def download_file(self, bucket, object_name, file_name):
        """
        Function responsible for downloading files from a server
        :param bucket: Bucket to download files from
        :param object_name: name of the object to be downloaded
        :param file_name: name of the file to be downloaded
        :return: boolean
        """

        if not bucket or not file_name or not object_name:
            raise ValueError("Bucket, object_name, and file_name must all be provided")

        try:
            self.s3_client.download_file(bucket, object_name, file_name)
        except ClientError as e:
            print(f"Client error: {str(e)}")
            return False
        except Exception as e:
            print(f"ERROR : (download File){str(e)}")
            return False
        return True



    def write_file(self, data, bucket, key):
        """
        Function Responsible for writing data to S3 file
        :param file: File with written content
        :param bucket: bucket to which fiie is uploaded to
        :param key: Unique key to find the file
        :return: Bool
        """
        if not data or not bucket or not key:
            raise ValueError("Data, bucket and key is required!")

        data = json.dumps(data).encode('UTF-8')

        try:
            self.s3_client.put_object(Body=data, Bucket=bucket, Key=key)
        except ClientError as e:
            print(f"Client Error: {str(e)}")
            return False
        except Exception as e:
            print(f"ERROR (Write File) : {str(e)}")
            return False
        return True


    def read_file(self, bucket, key):
        """
        Function responsible for reading S3 object
        :param bucket: Bucket name
        :param key: Key to ie the S3 object on the
        :return: Object data
        """

        if not bucket and not key:
            raise ValueError("Bucket or Key cannot be empty")

        try:
            s3_client = self.s3_client
            response = s3_client.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read()
            return data
        except ClientError:
            print(f"Client Error occurred")
            return False
        except Exception as e:
            print(f"ERROR (Read File): {str(e)}")






### DRIVER CODE  ###
#  Phase 1 : Creating Bucket
#create_bucket(bucket_name="test-bucket-mumbai-s3", region = 'ap-south-1' )

# Phase 2 : List Bucket
boto_obj = SimpleStorageService()
#boto_obj.list_buckets()

# Phase 3 : Adding Objects
#booldata = boto_obj.upload_object(file_name=r"D:\ORIM\CLOUD\AWS_BoTo3\files\Anya Polytech &_C.xlsx", bucket="test-bucket-mumbai-s3")

# Phase 4 : Downloading Objects
#booldata = boto_obj.download_file(bucket="test-bucket-mumbai-s3", object_name="Anya Polytech &_C.xlsx", file_name=r"D:\Anya Polytech &_C.xlsx")

# Phase 5 : Writing file
#data = {"name":"Raj Adhikari"}
#booldata = boto_obj.write_file(data=data, bucket='test-bucket-mumbai-s3', key="my-name")
"""if booldata:
    print("File WRitten")
else:
    print("File Not WRitten")"""

response = boto_obj.read_file(bucket='test-bucket-mumbai-s3', key='my-name')
print(f"DATA: {str(response)}")
