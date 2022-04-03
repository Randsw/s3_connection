from s3_module import S3_storage
import os


def minio_connect():
    bucketname = 'mybacket'
    my_s3 = S3_storage('127.0.0.1:9000', '12345678', 'password')
    return my_s3, bucketname

def prepare_test_data(my_s3, bucketname):
    my_s3.create_bucket(bucketname)
    filenames = ["./test_storage/1.txt", "./test_storage/2.txt", "./test_storage/3.txt"]
    os.makedirs(os.path.dirname(filenames[0]), exist_ok=True)
    for i, filename in enumerate(filenames):
        with open(filename, "w") as f:
            f.write(f"This is file number {i}")
    my_s3.put_objects(filenames, bucketname)

def test_create_bucket():
    my_s3, bucketname = minio_connect()
    my_s3.create_bucket(bucketname)

def test_delete_bucket():
    my_s3, bucketname = minio_connect()
    my_s3.create_bucket(bucketname)
    my_s3.delete_bucket(bucketname)
    my_s3.delete_bucket('not_exist_bucket')

def test_list_bucket():
    my_s3, bucketname = minio_connect()
    my_s3.create_bucket(bucketname)
    buckets = my_s3.list_bucket()
    assert buckets == ["mybacket"]

def test_put_objects():
    my_s3, bucketname = minio_connect()
    prepare_test_data(my_s3, bucketname)
    s3_objects = [object.object_name for object in my_s3.list_objects(bucketname)]
    assert s3_objects == ["1.txt", "2.txt", "3.txt"]

def test_get_objects():
    my_s3, bucketname = minio_connect()
    prepare_test_data(my_s3, bucketname)
    s3_objects = [object.object_name for object in my_s3.list_objects(bucketname)]
    os.makedirs(os.path.dirname("./test_storage2"), exist_ok=True)
    my_s3.get_objects(s3_objects, "./test_storage2", bucketname)
    assert sorted(os.listdir('./test_storage2')) == ["1.txt", "2.txt", "3.txt"]

def test_delete_objects():
    my_s3, bucketname = minio_connect()
    prepare_test_data(my_s3, bucketname)
    s3_objects = [object.object_name for object in my_s3.list_objects(bucketname)]
    my_s3.delete_objects(s3_objects, bucketname)
    assert [object.object_name for object in my_s3.list_objects(bucketname)] == []

def test_list_objects():
    my_s3, bucketname = minio_connect()
    prepare_test_data(my_s3, bucketname)
    s3_objects = [object.object_name for object in my_s3.list_objects(bucketname)]
    assert sorted(s3_objects) == ["1.txt", "2.txt", "3.txt"]