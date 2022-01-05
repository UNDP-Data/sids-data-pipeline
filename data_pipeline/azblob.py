from azure.storage.blob.aio import ContainerClient
from urllib.parse import urlparse
import logging
import os

logger = logging.getLogger(__name__)







def get_container_client(sas_url=None):
    assert sas_url is not None, f'sas_url is required to upload/download data from AZ blob container'
    try:
        return ContainerClient.from_container_url(sas_url)
    except Exception as e:
        logger.error(f'failed to create an azure.storage.blob.ContainerClient object from {sas_url}')
        raise



async def localfile2azureblob(container_client_instance=None, src=None, dst_blob_name=None,  overwrite=False, max_concurrency=8):

    """
    Async upload a local file to Azure container
    :param container_client_instance: instance of azure.storage.blob.aio.ContainerClient
    :param src: str, the path of the file
    :param dst_blob_name: str, the name of the uploaded blob. The file content will be stored in AZ under this name
    :param overwrite: bool, default=False, flag to force uploading an existing file
    :param max_concurrency, default = 8, the max nr of parallel conections to use if the file to upload is
    larger than 64 MB
    :return: None
    """

    parsed_src_url = urlparse(src)

    if not dst_blob_name:
        _, dst_blob_name = os.path.split(parsed_src_url.path)

    assert dst_blob_name not in [None, '', ' '], f'Invalid destination blob name {dst_blob_name}'
    try:

        async with container_client_instance:
                with open(src, 'rb') as data:
                    await container_client_instance.upload_blob(name=dst_blob_name, data=data,
                                                                blob_type='BlockBlob', overwrite=overwrite,
                                                                max_concurrency=max_concurrency)

        logger.info(f'{src} was uploaded as {dst_blob_name}')
    except Exception as e:
        logger.error(f'Failed to upload {src} to {container_client_instance.url}')
        raise



async def azureblob2localfile(container_client_instance=None, blob_name=None, dst_file=None):

    """
    Download a blob from an Azure blob container to local disk
    :param container_client_instance: instance of azure.storage.blob.aio.ContainerClient
    :param blob_name: str, name of the blob ot be downloaded
    :param dst_file: str, the full path to the
    :return:
    """
    assert dst_file not in [None, ''], f'invalid destination file {dst_file}'
    assert  os.path.isabs(dst_file), 'dst_file must be an absolute path'

    try:

        async with container_client_instance:
            with open(dst_file, 'wb') as dstf:
                stream = await container_client_instance.download_blob(blob_name)
                await stream.readinto(dstf)

        logger.info(f'{dst_file} was downloaded from {blob_name}')
    except Exception as e:
        logger.error(f'Failed to download {dst_file} from {blob_name} blob')
        raise




if __name__ == '__main__':
    import asyncio

    write_sas_url = 'https://undpngddlsgeohubdev01.blob.core.windows.net/test?sp=racwdl&st=2022-01-05T20:59:44Z&se=2023-01-06T04:59:44Z&spr=https&sv=2020-08-04&sr=c&sig=MkEoynTO0ftlLH95zq%2BXgjWl1%2F8um9OiYo1hpd6ufwE%3D'
    remote_file = 'https://drive.google.com/uc?export=download&id=1_kL7Iq4yFus4DKbgbsw6yUjMKn7QsB7o'
    #remote_file = 'https://popp.undp.org/UNDP_POPP_DOCUMENT_LIBRARY/Public/HR_Non-Staff_International%20Personnel%20Services%20Agreement_IPSA.docx#:~:text=The%20International%20Personnel%20Services%20Agreement,under%20a%20services%2Dbased%20contract.&text=Such%20contracts%20are%20then%20administered%20by%20UNDP.'
    local_file = '/data/sids/attribute_list_raw.csv'
    local_file = '/data/undp/zambia/zambia_energyaccess_poverty.csv'

    cc = get_container_client(sas_url=write_sas_url)

    #asyncio.run(localfile2azureblob(container_client_instance=cc, src=local_file, overwrite=True))
    asyncio.run(azureblob2localfile(container_client_instance=cc,blob_name='zambia_energyaccess_poverty.csv',dst_file='a'))

    #await cc.close()