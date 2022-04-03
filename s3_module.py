from minio import Minio
from minio.error import S3Error
import os
from s3_logger import S3Logger


class S3_storage:
    """
    S3_storage Class
    self.address    - minio adres
    self.access_key - minio access key - aws s3 like
    self.secret_key - minio secret key - aws s3 like
    self.secure     - enable TLS
    method list_bucket() - list all buckets
    method create_bucket(self, bucket_name) - create bucket
    method delete_bucket(self, bucket_name) - delete bucket
    method put_objects(self, objects, bucket_name) - Put file on backet
    method get_objects(self, objects, path, bucket_name) - Get object from bucket and put in filesystem by path
    method delete_objects(self, objects, bucketname) - Delete objects in bucket
    method list_objects(self, bucket_name, prefix) - List all object in bucket using prefix
    """

    def __init__(self, address, access_key, secret_key, secure=False):
        self.address = address
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.client = Minio(self.address, access_key=self.access_key, secret_key=self.secret_key, secure=self.secure)
        self.S3logger = S3Logger("minio")

    def create_bucket(self, bucket_name):
        """Create bucket in s3

        Parameters:
        bucketname (string): Name of bucket

        Returns:
        None

        """
        try:
            self.client.make_bucket(bucket_name)
            self.S3logger.logger.info(f'Create bucket - {bucket_name}')
        except S3Error as err:
            self.S3logger.logger.error(err)
    
    def delete_bucket(self, bucket_name):
        """Delete bucket in s3

        Parameters:
        bucketname (string): Name of bucket

        Returns:
        None

        """
        try:
            self.client.remove_bucket(bucket_name)
            self.S3logger.logger.info(f'Delete bucket - {bucket_name}')
        except S3Error as err:
            self.S3logger.logger.error(err)

    def list_bucket(self):
        """List all bucket in s3

        Parameters:
        None

        Returns:
        List: Names of bucket in s3

        """
        return self.client.list_buckets()

    def put_objects(self, objects, bucket_name):
        """Put object in bucket

        Parameters:
        objects (List): List of file paths to upload to bucket

        Returns:
        None

        """
        for object in objects:
            try:
                self.client.fput_object(bucket_name, os.path.basename(object), object)
                self.S3logger.logger.info(f'Put {object} in {bucket_name} bucket')
            except S3Error as err:
                self.S3logger.logger.error(err)

    def get_objects(self, objects, path, bucket_name):
        """Get objects from bucket

        Parameters:
        objects (List): Objects names in bucket
        path (string): Filepath ti save objects
        bucketname (string): Name of bucket

        Returns:
        None

        """
        for object in objects:
            try:
                self.client.fget_object(bucket_name, object, os.path.join(path, object))
                self.S3logger.logger.info(f'Get {object} to {path}')
            except S3Error as err:
                self.S3logger.logger.error(err)

    def delete_objects(self, objects, bucketname):
        """Delete objects in bucket

        Parameters:
        objects (List): List of object name to delete from bucket
        bucketname (string): Name of bucket

        Returns:
        None

        """
        for object in objects:
            try:
                self.client.remove_object(bucketname, object)
                self.S3logger.logger.info(f'Delete {object} from {bucketname} bucket')
            except S3Error as err:
                self.S3logger.logger.error(err)


    def list_objects(self, bucket_name, prefix=""):
        """List objects in bucket

        Parameters:
        bucketname (string): Name of bucket
        prefix (string): object name starts with prefix.

        Returns:
        Minio._list_objects: An iterator of Object with information
        
        """
        return self.client.list_objects(bucket_name, prefix, recursive=True)


if __name__ == "__main__":
    fileobjects = ["./test_storage/1.txt", "./test_storage/2.txt", "./test_storage/3.txt"]
    bucketname = 'mybacket'

    my_s3 = S3_storage('127.0.0.1:9000', '12345678', 'password')
    
    my_s3.create_bucket(bucketname)

    buckets = my_s3.list_bucket()
    for bucket in buckets:
        print(bucket.name, bucket.creation_date)

    my_s3.put_objects(fileobjects, bucketname)

    s3_objects = [object.object_name for object in my_s3.list_objects(bucketname)]

    my_s3.get_objects(s3_objects, "./test_storage2", bucketname)

    my_s3.delete_objects(s3_objects, bucketname)

    my_s3.delete_bucket(bucketname)
