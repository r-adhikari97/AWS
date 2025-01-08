import boto3
import os
from botocore.exceptions import ClientError

class SimpleStorageService:
    """
    Class responsible for handling  S3 Buckets
    """
    def __init__(self, region='ap-south-1'):
        self.region = region
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
                s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
            else:
                s3_client = boto3.client('s3', region_name=self.region)
                s3_client.create_bucket(Bucket=bucket_name)
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
            reponse = s3_client.upload_file(file_name, bucket, object_name)
            if reponse:
                print(f"{file_name} file uploaded to {bucket} buket successfully!")
        except ClientError as e:
            print(f"Client Error: {str(e)}")
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
        except Exception as e:
            print(f"ERROR : (download File){str(e)}")
            return False
        return True


### DRIVER CODE  ###
#  Phase 1 : Creating Bucket
#create_bucket(bucket_name="test-bucket-mumbai-s3", region = 'ap-south-1' )

# Phase 2 : List Bucket
boto_obj = SimpleStorageService()
boto_obj.list_buckets()

# Phase 3 : Adding Objects
#booldata = boto_obj.upload_object(file_name=r"D:\ORIM\CLOUD\AWS_BoTo3\files\Anya Polytech &_C.xlsx", bucket="test-bucket-mumbai-s3")

# Phase 4 : Downloading Objects
booldata = boto_obj.download_file(bucket="test-bucket-mumbai-s3", object_name="Anya Polytech &_C.xlsx", file_name=r"D:\Anya Polytech &_C.xlsx")
if booldata:
    print("File Dwonloaded")
else:
    print("File Not Dwonloaded")