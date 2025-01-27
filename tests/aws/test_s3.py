import pytest
from data_engineering_toolkit.aws.s3 import remove_s3_prefix
from data_engineering_toolkit.aws.s3 import get_bucket_name
from data_engineering_toolkit.aws.s3 import remove_filename_from_url
from data_engineering_toolkit.aws.s3 import get_object_key
from data_engineering_toolkit.aws.s3 import get_file_name


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


# def get_file_extension(url: str) -> str:
#     """ Retorna a extensao do arquivo a partir de uma url s3 """
#     return remove_s3_prefix(url).rsplit(".", maxsplit=1)[1]
#
#
# def get_database_from_url(url: str) -> str:
#     """ Retorna o banco de dados de uma tabela a partir de uma url s3 """
#     return remove_s3_prefix(url).split("/")[2]
#
#
# def get_table_name_from_url(url: str) -> str:
#     """ Retorna o nome da tabela a partir de uma url s3 """
#     return remove_s3_prefix(url).split("/")[3]
#
#
# def get_partition_path_from_url(url: str) -> str:
#     """ Retorna o caminho da particao a partir de uma url s3 """
#     return remove_s3_prefix(url).split('/', maxsplit=4)[4]
#
#
# def get_partition_fields_and_values_from_url(url: str) -> dict:
#     """ Retorna um dicionário com os campos e valores da partição """
#     partitions = remove_filename_from_url(remove_s3_prefix(url)).split('/')[4:]
#     fields_and_values = OrderedDict()
#     for partition in partitions:
#         field, value = partition.split('=')
#         fields_and_values[field] = value
#     return fields_and_values
#
#
# def get_partition_specification_from_url(url: str) -> str:
#     """
#     Retorna as especificações de partição a partir de uma url s3
#     Exemplo:
#         url = s3://bucket/databases/dba/tba/c1=1/c2=2
#         retorna: partition(c1='1', c2='2')
#     Retorno: str
#     """
#     partition_dict = get_partition_fields_and_values_from_url(url)
#     specification = ', '.join([f"{field}='{value}'" for field, value in list(partition_dict.items())])
#     return f"partition({specification})"
#
#
# def get_table_path_from_url(url: str) -> str:
#     """ Retorna o caminho da tabela de uma url s3 """
#     return "/".join(remove_s3_prefix(url).split("/", maxsplit=4)[:4])
#
#
# def get_partition_info_from_url(url: str) -> dict:
#     """ Retorna dicionario com informações da partição e tabela """
#     info = {
#         'bucket': get_bucket_name(url),
#         'database': get_database_from_url(url),
#         'table_path': get_table_path_from_url(url),
#         'table_name': get_table_name_from_url(url),
#         'partition_specification': get_partition_specification_from_url(url),
#         'partition_fields_and_values': get_partition_fields_and_values_from_url(url)
#     }
#     return info
#
#
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
#     file_key = get_file_key(url)
#     s3_object = s3_resource.Object(bucket_name, file_key)
#     file_size = s3_object.content_length
#     return file_size
