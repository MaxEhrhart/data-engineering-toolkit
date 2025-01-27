# encoding: latin1
"""
S3 Utilities for file management (download, upload, etc.) and s3 url functions
"""
import os
from collections import OrderedDict
import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore


def remove_s3_prefix(url: str) -> str:
    """ Remove o prefixo s3 | s3a | s3n de uma url s3 """
    return url.replace("s3a://", "").replace("s3n://", "").replace("s3://", "").rstrip("/")


def get_bucket_name(url: str) -> str:
    """ Retorna o nome do bucket a partir de uma url s3 """
    return remove_s3_prefix(url).split("/", maxsplit=1)[0]


def remove_filename_from_url(url: str) -> str:
    """ Remove o nome do arquivo de uma string s3 """
    if "." in remove_s3_prefix(url):
        return url.rsplit("/", maxsplit=1)[0]
    else:
        return url


def get_file_key(url: str) -> str:
    """ Retorna a key do caminho a partir de uma url s3 """
    try:
        return remove_s3_prefix(url).split("/", maxsplit=1)[1]
    except IndexError:
        return remove_s3_prefix(url)


def get_file_name(url: str) -> str:
    """ Retorna o nome do arquivo a partir de uma url s3 """
    return remove_s3_prefix(url).rsplit("/", maxsplit=1)[1]


def get_file_extension(url: str) -> str:
    """ Retorna a extensao do arquivo a partir de uma url s3 """
    return remove_s3_prefix(url).rsplit(".", maxsplit=1)[1]


def get_database_from_url(url: str) -> str:
    """ Retorna o banco de dados de uma tabela a partir de uma url s3 """
    return remove_s3_prefix(url).split("/")[2]


def get_table_name_from_url(url: str) -> str:
    """ Retorna o nome da tabela a partir de uma url s3 """
    return remove_s3_prefix(url).split("/")[3]


def get_partition_path_from_url(url: str) -> str:
    """ Retorna o caminho da particao a partir de uma url s3 """
    return remove_s3_prefix(url).split('/', maxsplit=4)[4]


def get_partition_fields_and_values_from_url(url: str) -> dict:
    """ Retorna um dicion?rio com os campos e valores da parti??o """
    partitions = remove_filename_from_url(remove_s3_prefix(url)).split('/')[4:]
    fields_and_values = OrderedDict()
    for partition in partitions:
        field, value = partition.split('=')
        fields_and_values[field] = value
    return fields_and_values


def get_partition_specification_from_url(url: str) -> str:
    """
    Retorna as especifica??es de parti??o a partir de uma url s3
    Exemplo:
        url = s3://bucket/databases/dba/tba/c1=1/c2=2
        retorna: partition(c1='1', c2='2')
    Retorno: str
    """
    partition_dict = get_partition_fields_and_values_from_url(url)
    specification = ', '.join([f"{field}='{value}'" for field, value in list(partition_dict.items())])
    return f"partition({specification})"


def get_table_path_from_url(url: str) -> str:
    """ Retorna o caminho da tabela de uma url s3 """
    return "/".join(remove_s3_prefix(url).split("/", maxsplit=4)[:4])


def get_partition_info_from_url(url: str) -> dict:
    """ Retorna dicionario com informa??es da particao e tabela """
    info = {
        'bucket': get_bucket_name(url),
        'database': get_database_from_url(url),
        'table_path': get_table_path_from_url(url),
        'table_name': get_table_name_from_url(url),
        'partition_specification': get_partition_specification_from_url(url),
        'partition_fields_and_values': get_partition_fields_and_values_from_url(url)
    }
    return info


def list_s3_files(url):
    """ Retorna a lista de arquivos com '.' a partir de uma url s3 """
    s3_client = boto3.client('s3')
    bucket = get_bucket_name(url)
    prefix = url.replace(bucket, '').replace('s3:///', '')
    response = s3_client.list_objects_v2(Bucket=bucket, StartAfter=prefix)
    if 'Contents' not in response:
        print("No files found.")
        return []
    return [f"s3://{bucket}/{file['Key']}" for file in response['Contents'] if '.' in file['Key']]


def get_file_size(url: str) -> int:
    """ Retorna o tamanho do arquivo no s3 em bytes """
    s3_resource = boto3.resource('s3')
    bucket_name = get_bucket_name(url)
    file_key = get_file_key(url)
    s3_object = s3_resource.Object(bucket_name, file_key)
    file_size = s3_object.content_length
    return file_size


def download_file(url: str, to: str = '.') -> None:
    """ Baixa o arquivo para o local a partir de uma url s3 """
    session = boto3.Session(region_name="us-east-1")
    s3_client = session.client('s3')
    bucket_name = get_bucket_name(url)
    file_key = get_file_key(url)
    file_name = get_file_name(url)
    file_name = f'{to}/{file_name}' if to == '.' else to
    s3_client.download_file(bucket_name, file_key, file_name)


def download_directory(bucket_name, s3_folder, local_dir=None):
    """
    Download the contents of a folder directory
    Args:
        bucket_name: the name of the s3 bucket
        s3_folder: the folder path in the s3 bucket
        local_dir: a relative or absolute directory path in the local file system
    Example: download_directory('digio-datalake-artifacts', 'pyspark/utilities', 'utilities')
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=s3_folder):
        target = obj.key if local_dir is None else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target), exist_ok=True)
        if obj.key[-1] == '/':
            continue
        bucket.download_file(obj.key, target)


def upload_file(local: str, url: str) -> bool:
    """ Sobe o arquivo local para um bucket s3 """
    session = boto3.Session(region_name="us-east-1")
    s3_client = session.client('s3')
    bucket_name = get_bucket_name(url)
    file_key = get_file_key(url)
    try:
        print(f'Uploading file: {local}: {url}')
        s3_client.upload_file(local, bucket_name, file_key)
        print(f'file completed: {url}')
    except ClientError as e:
        print(e)
        return False
    return True


def read_file(url: str, decode: bool = True, encoding: str = 'utf-8') -> str:
    """ Retorna o conteudo do arquivo decodificado ou em bytes se especificado """
    try:
        s3_resource = boto3.resource('s3')
        bucket_name = get_bucket_name(url)
        file_key = get_file_key(url)
        if decode:
            return s3_resource.Object(bucket_name, file_key).get()['Body'].read().decode(encoding)
        else:
            return s3_resource.Object(bucket_name, file_key).get()['Body'].read()
    except ClientError as e:
        print(f"Error: {e.response['Error']['Message']}")
        raise e
    except UnicodeDecodeError as e:
        print(f"Error: {e}")
        raise e


def copy_file(source: str, target: str, extra_args: dict = None) -> bool:
    """ Copia o arquivo do source para o target """
    try:
        s3 = boto3.resource('s3')
        copy_source = {
            'Bucket': get_bucket_name(source),
            'Key': get_file_key(source)
        }
        s3.meta.client.copy(
            copy_source,
            get_bucket_name(target),
            get_file_key(target),
            ExtraArgs=extra_args
        )
    except ClientError as e:
        print(e)
        return False
    return True


def delete_file(url, show: bool = False) -> bool:
    """ Remove o arquivo. """
    try:
        s3_client = boto3.client('s3')
        bucket = get_bucket_name(url)
        key = get_file_key(url)
        response = s3_client.delete_object(Bucket=bucket, Key=key)
        print(response) if show else None
        return response['ResponseMetadata']['HTTPStatusCode'] in (200, 204)
    except ClientError as e:
        print(e)
        return False


def move_file(source: str, target: str, show: bool = False) -> bool:
    print(f"Moving {source} to {target}") if show else None
    assert copy_file(source, target) and delete_file(source)
    return True


def delete_directory(url) -> bool:
    """ Remove o diretorio """
    try:
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(get_bucket_name(url))
        prefix = get_file_key(url)
        objects_to_delete = []
        for obj in bucket.objects.filter(Prefix=prefix):
            objects_to_delete.append({'Key': obj.key})
        bucket.delete_objects(Delete={'Objects': objects_to_delete})
        return True
    except Exception as e:
        print(e)
        return False


def create_s3_file(url: str, content: str):
    s3 = boto3.resource('s3', region_name='us-east-1')
    print(f'Creating file {url}.')
    s3.Object(get_bucket_name(url), get_file_key(url)).put(Body=content)
    print(f'File succesfully created.')


if __name__ == '__main__':
    pass