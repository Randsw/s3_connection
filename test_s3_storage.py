from s3_module import S3Storage
import os


def minio_connect():
    bucket_name = 'my-bucket'
    my_s3 = S3Storage('127.0.0.1:9000', '12345678', 'password')
    return my_s3, bucket_name


def prepare_test_data(my_s3, bucket_name):
    my_s3.create_bucket(bucket_name)
    filenames = ["./test_storage/1.txt", "./test_storage/2.txt", "./test_storage/3.txt"]
    os.makedirs(os.path.dirname(filenames[0]), exist_ok=True)
    for i, filename in enumerate(filenames):
        with open(filename, "w") as f:
            f.write(f"This is file number {i}")
    my_s3.put_objects(filenames, bucket_name)


def test_create_bucket():
    my_s3, bucket_name = minio_connect()
    my_s3.create_bucket(bucket_name)


def test_delete_bucket():
    my_s3, bucket_name = minio_connect()
    my_s3.create_bucket(bucket_name)
    my_s3.delete_bucket(bucket_name)
    my_s3.delete_bucket('not_exist_bucket')


def test_list_bucket():
    my_s3, bucket_name = minio_connect()
    my_s3.create_bucket(bucket_name)
    buckets = my_s3.list_bucket()
    assert "my-bucket" in buckets


def test_put_objects():
    my_s3, bucket_name = minio_connect()
    prepare_test_data(my_s3, bucket_name)
    s3_objects = [s3_object.object_name for s3_object in my_s3.list_objects(bucket_name)]
    assert s3_objects == ["1.txt", "2.txt", "3.txt"]


def test_get_objects():
    my_s3, bucket_name = minio_connect()
    prepare_test_data(my_s3, bucket_name)
    s3_objects = [s3_object.object_name for s3_object in my_s3.list_objects(bucket_name)]
    os.makedirs(os.path.dirname("./test_storage2"), exist_ok=True)
    my_s3.get_objects(s3_objects, "./test_storage2", bucket_name)
    assert sorted(os.listdir('./test_storage2')) == ["1.txt", "2.txt", "3.txt"]


def test_delete_objects():
    my_s3, bucket_name = minio_connect()
    prepare_test_data(my_s3, bucket_name)
    s3_objects = [s3_object.object_name for s3_object in my_s3.list_objects(bucket_name)]
    my_s3.delete_objects(s3_objects, bucket_name)
    assert [s3_object.object_name for s3_object in my_s3.list_objects(bucket_name)] == []


def test_list_objects():
    my_s3, bucket_name = minio_connect()
    prepare_test_data(my_s3, bucket_name)
    s3_objects = [s3_object.object_name for s3_object in my_s3.list_objects(bucket_name)]
    assert sorted(s3_objects) == ["1.txt", "2.txt", "3.txt"]
