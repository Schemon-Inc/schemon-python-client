import os
from typing import Optional
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from schemon_python_client.spark.base.client import Client
from schemon_python_client.spark.credential_manager.s3_credential_manager import (
    S3CredentialManager,
)
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame


class S3Client(Client):
    def __init__(
        self,
        spark: SparkSession,
        credential_manager: S3CredentialManager,
        platform: str,
        format: str,
        region: str = "ca-central-1",
        driver_type: str = "s3a",
    ):
        """
        Initialize the S3Client with S3 credentials from the S3CredentialManager.
        :param s3_credential_manager: The S3CredentialManager instance that provides credentials.
        :param region: The AWS region where the S3 service is located.
        """
        super().__init__(
            spark=spark,
            provider="aws",
            name="s3",
            platform=platform,
            format=format,
            credential_manager=credential_manager,
        )
        self.region = region
        self.driver_type = driver_type
        self.boto3_client = self._initialize_boto3_client()

    def _initialize_boto3_client(self):
        """
        Initialize the boto3 client using credentials from S3CredentialManager and store it as a class property.
        """
        try:
            credentials = self.credential_manager.get_credentials()
            if not credentials:
                raise NoCredentialsError

            # Create and return an S3 client with the provided credentials
            return boto3.client(
                "s3",
                aws_access_key_id=credentials["access_key"],
                aws_secret_access_key=credentials["secret_access_key"],
                region_name=self.region,
            )
        except (NoCredentialsError, PartialCredentialsError):
            print("Error: Invalid or incomplete credentials.")
            return None

    def list_buckets(self):
        """
        List all S3 buckets available under the provided credentials.
        """
        if self.boto3_client:
            try:
                response = self.boto3_client.list_buckets()
                print("Buckets available:")
                for bucket in response["Buckets"]:
                    print(f" - {bucket['Name']}")
            except Exception as e:
                print(f"An error occurred: {str(e)}")

    def list_objects(
        self, bucket_name: str, prefix: str = "", recursive: bool = True
    ) -> dict:
        """
        List all objects and directories under the specified prefix (directory) in the S3 bucket.
        Add a 'type_' key with either 'file' or 'directory' for each object.

        :param bucket_name: Name of the S3 bucket.
        :param prefix: Prefix (directory) to list objects from. Default is root ('').
        :param recursive: If True, lists all objects recursively under the prefix.
        :return: A modified response dict with 'type_' key added to each object (file or directory).
        """
        if self.boto3_client:
            try:
                response_files = {
                    "IsTruncated": False,  # Will change based on response if there is pagination
                    "Contents": [],
                    "Name": bucket_name,
                    "Prefix": prefix,
                    "MaxKeys": 1000,
                    "CommonPrefixes": [],
                }
                continuation_token = None

                while True:
                    list_kwargs = {
                        "Bucket": bucket_name,
                        "Prefix": prefix,
                        "Delimiter": "" if recursive else "/",
                    }

                    if continuation_token:
                        list_kwargs["ContinuationToken"] = continuation_token

                    response = self.boto3_client.list_objects_v2(**list_kwargs)

                    # Add 'type_' key: 'file' for regular files, 'directory' for directories
                    if "Contents" in response:
                        for obj in response["Contents"]:
                            obj_type = (
                                "directory" if obj["Key"].endswith("/") else "file"
                            )
                            response_files["Contents"].append(
                                {**obj, "type_": obj_type}
                            )

                    # Capture CommonPrefixes if recursive is False (for directories)
                    if "CommonPrefixes" in response:
                        for common_prefix in response["CommonPrefixes"]:
                            response_files["Contents"].append(
                                {"Key": common_prefix["Prefix"], "type_": "directory"}
                            )

                    # Check if pagination is needed
                    response_files["IsTruncated"] = response.get("IsTruncated", False)
                    continuation_token = response.get("NextContinuationToken")
                    if not continuation_token:
                        break

                return response_files

            except Exception as e:
                print(f"An error occurred: {str(e)}")
                return {}

    def upload_object(self, bucket_name: str, file_path: str, object_name: str = None):
        """
        Upload an object to the S3 bucket.
        :param bucket_name: Name of the S3 bucket.
        :param file_path: Path to the file to upload.
        :param object_name: S3 object name (optional). If not specified, the file name will be used.
        """
        if self.boto3_client:
            try:
                if object_name is None:
                    object_name = file_path.split(os.sep)[-1]
                self.boto3_client.upload_file(file_path, bucket_name, object_name)
                print(f"Uploaded {file_path} to {bucket_name}/{object_name}")
            except Exception as e:
                print(f"An error occurred: {str(e)}")

    def upload_directory(
        self, bucket_name: str, directory_path: str, s3_directory: str
    ):
        """
        Upload all files from a local directory to an S3 directory.
        :param bucket_name: Name of the S3 bucket.
        :param directory_path: Local directory path.
        :param s3_directory: S3 directory (prefix) to upload the files to.
        """
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, directory_path)
                s3_file_path = os.path.join(s3_directory, relative_path).replace(
                    "\\", "/"
                )
                self.upload_object(bucket_name, local_file_path, s3_file_path)

    def delete_object(self, bucket_name: str, object_name: str):
        """
        Delete an object from the S3 bucket.
        :param bucket_name: Name of the S3 bucket.
        :param object_name: Name of the object to delete from the S3 bucket.
        """
        if self.boto3_client:
            try:
                self.boto3_client.delete_object(Bucket=bucket_name, Key=object_name)
                print(f"Deleted object {object_name} from {bucket_name}.")
            except Exception as e:
                print(f"An error occurred: {str(e)}")

    def download_object(self, bucket_name: str, object_name: str, file_path: str):
        """
        Download an object from the S3 bucket to a specified local file path.
        :param bucket_name: Name of the S3 bucket.
        :param object_name: Name of the object to download.
        :param file_path: Local file path to save the downloaded object.
        """
        if self.boto3_client:
            try:
                self.boto3_client.download_file(bucket_name, object_name, file_path)
                print(f"Downloaded {object_name} from {bucket_name} to {file_path}")
            except Exception as e:
                print(f"An error occurred: {str(e)}")

    def download_directory(
        self, bucket_name: str, s3_directory: str, local_directory: str
    ):
        """
        Download all files from an S3 directory to a local directory.
        :param bucket_name: Name of the S3 bucket.
        :param s3_directory: S3 directory (prefix) to download the files from.
        :param local_directory: Local directory path to save the downloaded files.
        """
        if self.boto3_client:
            try:
                response = self.boto3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=s3_directory
                )
                if "Contents" in response:
                    for obj in response["Contents"]:
                        s3_file_path = obj["Key"]
                        relative_path = os.path.relpath(s3_file_path, s3_directory)
                        local_file_path = os.path.join(local_directory, relative_path)
                        local_dir = os.path.dirname(local_file_path)

                        # Create local directories if not existing
                        if not os.path.exists(local_dir):
                            os.makedirs(local_dir)

                        self.download_object(bucket_name, s3_file_path, local_file_path)
            except Exception as e:
                print(f"An error occurred: {str(e)}")

    def get_object_metadata(self, bucket_name: str, object_key: str):
        """
        Retrieve metadata for an S3 object, including the last modified timestamp.
        """
        if self.boto3_client:
            try:
                # Retrieve the object's metadata
                response = self.boto3_client.head_object(
                    Bucket=bucket_name, Key=object_key
                )

                # Extract relevant metadata fields
                metadata = {
                    "LastModified": response["LastModified"],
                    "ContentLength": response["ContentLength"],
                    "ContentType": response["ContentType"],
                    "ETag": response["ETag"],
                    "Metadata": response.get("Metadata", {}),  # Custom metadata
                }

                return metadata

            except self.boto3_client.exceptions.NoSuchKey:
                print(f"Object '{object_key}' not found in bucket '{bucket_name}'.")
                return None
            except Exception as e:
                print(f"Error retrieving metadata: {e}")
                return None

    def read(
        self,
        database: str = None,
        schema: str = None,
        table: str = None,
        columns: Optional[list[str]] = None,
        use_sql: bool = False,
    ) -> SparkDataFrame:
        """
        Reading from S3 should be using either from directories - reader/excel or reader/flatfile
        """
        NotImplemented
