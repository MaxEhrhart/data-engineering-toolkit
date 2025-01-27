from data_engineering_toolkit.aws.s3 import remove_s3_prefix
from data_engineering_toolkit.aws.s3 import get_bucket_name
from data_engineering_toolkit.aws.s3 import remove_filename_from_url
from data_engineering_toolkit.aws.s3 import get_object_key
from data_engineering_toolkit.aws.s3 import get_file_name
from data_engineering_toolkit.aws.s3 import get_file_extension


def test_remove_s3_prefix():
    url = 's3://bucket/prefix/object.txt'
    assert remove_s3_prefix(url) == 'bucket/prefix/object.txt'


def test_get_bucket_name():
    url = 's3://bucket/prefix/object.txt'
    assert get_bucket_name(url) == 'bucket'


def test_remove_filename_from_url():
    url = 's3://bucket/prefix/object.txt'
    assert remove_filename_from_url(url) == 's3://bucket/prefix'


def test_get_object_key():
    url = 's3://bucket/prefix/object.txt'
    assert get_object_key(url) == 'prefix/object.txt'


def test_get_file_name():
    url = 's3://bucket/prefix/object.txt'
    assert get_file_name(url) == 'object.txt'


def test_get_file_extension():
    url = 's3://bucket/prefix/object.txt'
    assert get_file_extension(url) == 'txt'


# def list_s3_files(url):
#     """ Retorna a lista de arquivos com '.' a partir de uma url s3 """
#     s3_client = boto3.client('s3')
#     bucket = get_bucket_name(url)
#     prefix = url.replace(bucket, '').replace('s3:///', '')
#     response = s3_client.list_objects_v2(Bucket=bucket, StartAfter=prefix)
#     if 'Contents' not in response:
#         print("No files found.")
#         return []
#     return [f"s3://{bucket}/{file['Key']}" for file in response['Contents'] if '.' in file['Key']]
#
#
# def get_file_size(url: str) -> int:
#     """ Retorna o tamanho do arquivo no s3 em bytes """
#     s3_resource = boto3.resource('s3')
#     bucket_name = get_bucket_name(url)
#     file_key = get_object_key(url)
#     s3_object = s3_resource.Object(bucket_name, file_key)
#     file_size = s3_object.content_length
#     return file_size
#
#
# def download_file(url: str, to: str = '.') -> None:
#     """ Baixa o arquivo para o local a partir de uma url s3 """
#     session = boto3.Session(region_name="us-east-1")
#     s3_client = session.client('s3')
#     bucket_name = get_bucket_name(url)
#     file_key = get_object_key(url)
#     file_name = get_file_name(url)
#     file_name = f'{to}/{file_name}' if to == '.' else to
#     s3_client.download_file(bucket_name, file_key, file_name)
#
#
# def download_directory(bucket_name, s3_folder, local_dir=None):
#     """
#     Download the contents of a folder directory
#     Args:
#         bucket_name: the name of the s3 bucket
#         s3_folder: the folder path in the s3 bucket
#         local_dir: a relative or absolute directory path in the local file system
#     Example: download_directory('datalake-artifacts', 'pyspark/utilities', 'utilities')
#     """
#     s3 = boto3.resource('s3')
#     bucket = s3.Bucket(bucket_name)
#     for obj in bucket.objects.filter(Prefix=s3_folder):
#         target = obj.key if local_dir is None else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
#         if not os.path.exists(os.path.dirname(target)):
#             os.makedirs(os.path.dirname(target), exist_ok=True)
#         if obj.key[-1] == '/':
#             continue
#         bucket.download_file(obj.key, target)
#
#
# def upload_file(local: str, url: str) -> bool:
#     """ Sobe o arquivo local para um bucket s3 """
#     session = boto3.Session(region_name="us-east-1")
#     s3_client = session.client('s3')
#     bucket_name = get_bucket_name(url)
#     file_key = get_object_key(url)
#     try:
#         print(f'Uploading file: {local}: {url}')
#         s3_client.upload_file(local, bucket_name, file_key)
#         print(f'file completed: {url}')
#     except ClientError as e:
#         print(e)
#         return False
#     return True
#
#
# def read_file(url: str, decode: bool = True, encoding: str = 'utf-8') -> str:
#     """ Retorna o conteudo do arquivo decodificado ou em bytes se especificado """
#     try:
#         s3_resource = boto3.resource('s3')
#         bucket_name = get_bucket_name(url)
#         file_key = get_object_key(url)
#         if decode:
#             return s3_resource.Object(bucket_name, file_key).get()['Body'].read().decode(encoding)
#         else:
#             return s3_resource.Object(bucket_name, file_key).get()['Body'].read()
#     except ClientError as e:
#         print(f"Error: {e.response['Error']['Message']}")
#         raise e
#     except UnicodeDecodeError as e:
#         print(f"Error: {e}")
#         raise e
#
#
# def copy_file(source: str, target: str, extra_args: dict = None) -> bool:
#     """ Copia o arquivo do source para o target """
#     try:
#         s3 = boto3.resource('s3')
#         copy_source = {
#             'Bucket': get_bucket_name(source),
#             'Key': get_object_key(source)
#         }
#         s3.meta.client.copy(
#             copy_source,
#             get_bucket_name(target),
#             get_object_key(target),
#             ExtraArgs=extra_args
#         )
#     except ClientError as e:
#         print(e)
#         return False
#     return True
#
#
# def delete_file(url, show: bool = False) -> bool:
#     """ Remove o arquivo. """
#     try:
#         s3_client = boto3.client('s3')
#         bucket = get_bucket_name(url)
#         key = get_object_key(url)
#         response = s3_client.delete_object(Bucket=bucket, Key=key)
#         print(response) if show else None
#         return response['ResponseMetadata']['HTTPStatusCode'] in (200, 204)
#     except ClientError as e:
#         print(e)
#         return False
#
#
# def move_file(source: str, target: str, show: bool = False) -> bool:
#     print(f"Moving {source} to {target}") if show else None
#     assert copy_file(source, target) and delete_file(source)
#     return True
#
#
# def delete_directory(url) -> bool:
#     """ Remove o diretorio """
#     try:
#         s3_resource = boto3.resource('s3')
#         bucket = s3_resource.Bucket(get_bucket_name(url))
#         prefix = get_object_key(url)
#         objects_to_delete = []
#         for obj in bucket.objects.filter(Prefix=prefix):
#             objects_to_delete.append({'Key': obj.key})
#         bucket.delete_objects(Delete={'Objects': objects_to_delete})
#         return True
#     except Exception as e:
#         print(e)
#         return False
#
#
# def create_s3_file(url: str, content: str):
#     s3 = boto3.resource('s3', region_name='us-east-1')
#     print(f'Creating file {url}.')
#     s3.Object(get_bucket_name(url), get_object_key(url)).put(Body=content)
#     print(f'File succesfully created.')
