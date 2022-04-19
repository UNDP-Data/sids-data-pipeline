from .utils import scantree, mkdir_recursive, slicer, count
from azure.storage.blob.aio import ContainerClient
from urllib.parse import urlparse
import logging
import os
import time
import asyncio
from tqdm import tqdm
logger = logging.getLogger(__name__)


def atimeit(func):
    """
    A decorator to measure the execution duration of async functions
    :param func:
    :return:
    """

    async def process(func, *args, **params):
        return await func(*args, **params)

    async def helper(*args, **params):
        start = time.time()
        result = await process(func, *args, **params)
        elapsed = time.time() - start
        logger.info(f'{func.__name__} took {elapsed} secs')
        return result

    return helper


def get_container_client(sas_url=None):
    assert sas_url is not None, f'sas_url is required to upload/download data from AZ blob container'
    try:
        return ContainerClient.from_container_url(sas_url)
    except Exception as e:
        logger.error(
            f'failed to create an azure.storage.blob.ContainerClient object from {sas_url}')
        raise


class HandyContainerClient():

    def __init__(self, sas_url=None):
        assert sas_url is not None, f'sas_url is required to upload/download data from AZ blob container'
        self.sas_url = sas_url
        self.cclient = ContainerClient.from_container_url(self.sas_url)

    async def __aenter__(self):
        return self.cclient

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cclient.close()


class FancyContainerClient():

    def __init__(self, sas_url=None):
        assert sas_url is not None, f'sas_url is required to upload/download data from AZ blob container'
        self.sas_url = sas_url
        self.cclient = ContainerClient.from_container_url(self.sas_url)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self.cclient.close())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cclient.close()

    async def list_blobs_async(self, name=None):
        """
        Lists blobs whose name start with name
        :param name: str
        :return:
        """

        async for blob in self.cclient.list_blobs(name_starts_with=name):
            yield blob

    async def async2sync(self, name=None):
        return [item async for item in self.list_blobs_async(name=name)]

    def listblobs(self, name=None):
        """
        Lists blobs whose name start with name
        :param name: str
        :return:
        """

        yield from asyncio.run(self.async2sync(name=name))


async def localfile2azureblob(container_client_instance=None, src=None, dst_blob_name=None,  overwrite=False, max_concurrency=8):

    """
    Asynchronously upload a local file to Azure container
    :param container_client_instance: instance of azure.storage.blob.aio.ContainerClient
    :param src: str, the path of the file
    :param dst_blob_name: str, the name of the uploaded blob. The file content will be stored in AZ under this name
    :param overwrite: bool, default=False, flag to force uploading an existing file
    :param max_concurrency, default = 8, maximum number of parallel connections to use when the blob size exceeds 64MB

    :return: None
    """

    parsed_src_url = urlparse(src)

    if not dst_blob_name:
        _, dst_blob_name = os.path.split(parsed_src_url.path)

    assert dst_blob_name not in [
        None, '', ' '], f'Invalid destination blob name {dst_blob_name}'
    try:

        async with container_client_instance:
            with open(src, 'rb') as data:
                await container_client_instance.upload_blob(name=dst_blob_name, data=data,
                                                            blob_type='BlockBlob', overwrite=overwrite,
                                                            max_concurrency=max_concurrency)

        logger.info(f'{src} was uploaded as {dst_blob_name}')
    except Exception as e:
        logger.error(
            f'Failed to upload {src} to {container_client_instance.url}')
        raise


async def upload_file(container_client_instance=None, src=None, dst_blob_name=None,  overwrite=False, max_concurrency=8):

    """
    Async upload a local file to Azure container.
    Do not use directly. This function is meant to be used inside a loop where many files
    are uploaded asynchronously
    :param container_client_instance: instance of azure.storage.blob.aio.ContainerClient
    :param src: str, the path of the file
    :param dst_blob_name: str, the name of the uploaded blob. The file content will be stored in AZ under this name
    :param overwrite: bool, default=False, flag to force uploading an existing file
    :param max_concurrency, default = 8, maximum number of parallel connections to use when the blob size exceeds 64MB

    :return: None
    """

    parsed_src_url = urlparse(src)

    if not dst_blob_name:
        _, dst_blob_name = os.path.split(parsed_src_url.path)

    assert dst_blob_name not in [
        None, '', ' '], f'Invalid destination blob name {dst_blob_name}'

    with open(src, 'rb') as data:
        blob_client = await container_client_instance.upload_blob(name=dst_blob_name, data=data,
                                                                  blob_type='BlockBlob', overwrite=overwrite,
                                                                  max_concurrency=max_concurrency)
        logger.debug(f'{src} was uploaded as {dst_blob_name}')
        return blob_client, src


async def azureblob2localfile(container_client_instance=None, blob_name=None, dst_file=None):

    """
    Download a blob from an Azure blob container to local disk

    Do not use directly. this function is meant to be used inside a loop where many files
    are downloaded asynchronously

    :param container_client_instance: instance of azure.storage.blob.aio.ContainerClient
    :param blob_name: str, name of the blob ot be downloaded
    :param dst_file: str, the full path to the file where the blob will be downloaded
    :return: None
    """
    assert dst_file not in [None, ''], f'invalid destination file {dst_file}'
    assert os.path.isabs(dst_file), 'dst_file must be an absolute path'

    try:

        async with container_client_instance:
            with open(dst_file, 'wb') as dstf:
                stream = await container_client_instance.download_blob(blob_name)
                await stream.readinto(dstf)

        logger.info(f'{dst_file} was downloaded from {blob_name}')
    except Exception as e:
        err_msg = f'Failed to download {dst_file} from {blob_name} blob.\n {e}'
        logger.error(err_msg)
        raise Exception(err_msg)


async def download_file(container_client_instance=None, blob_name=None, dst_file=None):

    """
    Download a blob from an Azure blob container to local disk
    :param container_client_instance: instance of azure.storage.blob.aio.ContainerClient
    :param blob_name: str, name of the blob ot be downloaded
    :param dst_file: str, the full path to the file where the blob will be downloaded
    :return: True, dst_file in case no exception is encountered
    """
    assert dst_file not in [None, ''], f'invalid destination file {dst_file}'
    assert os.path.isabs(dst_file), 'dst_file must be an absolute path'
    logger.info(f'Going to download {dst_file} from {blob_name}')
    with open(dst_file, 'wb') as dstf:
        stream = await container_client_instance.download_blob(blob_name)
        await stream.readinto(dstf)

    logger.info(f'{dst_file} was downloaded from {blob_name}')
    return True, dst_file


async def folder2azureblob(container_client_instance=None, src_folder=None, dst_blob_name=None,
                           overwrite=False, max_concurrency=8, timeout=None
                           ):
    """
    Asynchronously upload a local folder (including its content) to Azure blob container

    :param container_client_instance: instance of azure.storage.blob.aio.ContainerClient
    :param src_folder: str, full abs path to the folder to be uploaded
    :param dst_blob_name: str the name of the blob where the content fo the folder will be downloaded
    :param overwrite: bool, defaults to false, sepcifiy if an existing blob will be overwritten
    :param max_concurrency: int, maximum number of parallel connections to use when the blob size exceeds 64MB
    :param timeout, timeout in seconds to be applied to uploading all files in the folder.
    :return:
    """
    assert src_folder not in [
        None, '', '/'], f'src_folder={src_folder} is invalid'
    assert os.path.exists(
        src_folder), f'src_folder={src_folder} does not exist'
    assert os.path.isabs(
        src_folder), f'src_folder={src_folder} is not a an absolute path'
    assert os.path.isdir(
        src_folder), f'src_folder={src_folder} is not a directory'
    assert len(src_folder) > 1, f'src_folder={src_folder} is invalid'

    try:
        async with container_client_instance:
            prefix = os.path.split(
                src_folder)[-1] if dst_blob_name is None else dst_blob_name
            r = scantree(src_folder)
            nfiles = count(r)
            nchunks = nfiles//100 + 1
            n = 0
            r = scantree(src_folder)
            with tqdm(total=nchunks, desc="Uploading ... ", initial=0, unit_scale=True,
                      colour='green') as pbar:
                for chunk in slicer(r, 100):
                    ftrs = list()
                    #logger.info(f'Uploading file chunk no {n} from {nchunks} - {n / nchunks * 100:.2f}%')

                    for local_file in chunk:

                        if not local_file.is_file():
                            continue
                        blob_path = os.path.join(
                            prefix, os.path.relpath(local_file.path, src_folder))
                        #print(e.path, blob_path)
                        fut = asyncio.ensure_future(
                            upload_file(container_client_instance=container_client_instance,
                                        src=local_file.path, dst_blob_name=blob_path, overwrite=overwrite,
                                        max_concurrency=max_concurrency)
                        )
                        ftrs.append(fut)

                    done, pending = await asyncio.wait(ftrs, timeout=timeout, return_when=asyncio.ALL_COMPLETED)
                    results = await asyncio.gather(*done, return_exceptions=True)
                    for res in results:
                        if type(res) == tuple:
                            blob_client, file_path_to_upload = res
                        else:  # error
                            logger.error(
                                f'{file_path_to_upload} was not uploaded successfully to {blob_client.blob_name}')
                            logger.error(res)

                    for failed in pending:
                        blob_client, file_path_to_upload = await failed
                        logger.debug(
                            f'Uploading {file_path_to_upload} to {container_client_instance.url} has timed out.')
                    pbar.update(1)
                    n += 1
    except Exception as err:
        logger.error(
            f'Failed to upload {src_folder} to {container_client_instance.url}')
        raise


async def download_folder_from_azure(container_client_instance=None, src_blob_name=None, dst_folder=None, timeout=None,
                                     strip_path=False):
    """
    Asynchronously download
    :param container_client_instance: instance of azure.storage.blob.aio.ContainerClient
    :param src_blob_name: str, the name of the blob/folder to download
    :param dst_folder: str, full absolute path to the folder where the folder/blob will be downloaded
    :param timeout, timeout in seconds to be applied to downloading all files in the folder.
    :param: strip_path, defaults to False, if True the files from thne src blob name will be
            saved without their relative path from Azure in thier name, that is directly into the dst_folder
    :return:
    """

    assert dst_folder not in [
        None, '', '/'], f'dst_folder={dst_folder} is invalid'
    assert os.path.exists(
        dst_folder), f'dst_folder={dst_folder} does not exist'
    assert os.path.isabs(
        dst_folder), f'dst_folder={dst_folder} is not a an absolute path'
    assert os.path.isdir(
        dst_folder), f'dst_folder={dst_folder} is not a directory'
    assert len(dst_folder) > 1, f'dst_folder={dst_folder} is invalid'

    ftrs = list()

    try:
        async with container_client_instance:

            blob_iter = container_client_instance.list_blobs(
                name_starts_with=src_blob_name)
            async for blob in blob_iter:
                if not strip_path:
                    dst_file = os.path.join(dst_folder, blob.name)
                else:
                    dst_file = os.path.join(
                        dst_folder, os.path.split(blob.name)[-1])
                mkdir_recursive(os.path.dirname(dst_file))
                if os.path.exists(dst_file) and blob.size == os.path.getsize(dst_file):
                    logger.info(
                        f'Skipping {blob.name} because it already exists as {dst_file}')
                    continue
                fut = asyncio.ensure_future(
                    download_file(container_client_instance=container_client_instance,
                                  blob_name=blob.name,
                                  dst_file=dst_file
                                  )
                )
                ftrs.append(fut)
            done, pending = await asyncio.wait(ftrs, timeout=timeout, return_when=asyncio.ALL_COMPLETED)
            results = await asyncio.gather(*done, return_exceptions=True)

            for res in results:

                if type(res) == tuple:
                    success, downloaded_file_path = res
                    logger.debug(
                        f'{downloaded_file_path} was successfully downloaded from {container_client_instance.url}')
                else:  # error
                    logger.error(
                        f'{downloaded_file_path} was not downloaded successfully from {container_client_instance.url}')
                    logger.error(res)

            for failed in pending:
                success, downloaded_file_path = await failed
                logger.info(
                    f'Downloading {downloaded_file_path} from {container_client_instance.url} has timed out.')

    except Exception as err:
        logger.error(err)
        raise


async def upload_mvts(sas_url=None, src_folder=None, dst_blob_name=None, timeout=30*60):
    """
    Asyn upload a folder to Azure blob
    :param sas_url: str, the SAS url
    :param src_folder: str, full abs path to the folder
    :param dst_blob_name: str, relative (to container) timeout of the
    :param timeout:
    :return:
    """

    async with HandyContainerClient(sas_url=sas_url) as cc:
        return await folder2azureblob(
            container_client_instance=cc,
            src_folder=src_folder,
            dst_blob_name=dst_blob_name,
            overwrite=True,
            timeout=timeout
        )


if __name__ == '__main__':

    logging.basicConfig()
    logger.setLevel('INFO')
    write_sas_url = 'https://undpngddlsgeohubdev01.blob.core.windows.net/test?sp=racwdl&st=2022-01-05T20:59:44Z&se=2023-01-06T04:59:44Z&spr=https&sv=2020-08-04&sr=c&sig=MkEoynTO0ftlLH95zq%2BXgjWl1%2F8um9OiYo1hpd6ufwE%3D'
    sas_url = 'https://undpngddlsgeohubdev01.blob.core.windows.net/sids?sp=racwdl&st=2022-01-06T21:09:27Z&se=2032-01-07T05:09:27Z&spr=https&sv=2020-08-04&sr=c&sig=XtcP1UUnboo7gSVHOXeTbUt0g%2FSV2pxG7JVgmZ8siwo%3D'
    remote_file = 'https://drive.google.com/uc?export=download&id=1_kL7Iq4yFus4DKbgbsw6yUjMKn7QsB7o'
    remote_file = 'https://popp.undp.org/UNDP_POPP_DOCUMENT_LIBRARY/Public/HR_Non-Staff_International%20Personnel%20Services%20Agreement_IPSA.docx#:~:text=The%20International%20Personnel%20Services%20Agreement,under%20a%20services%2Dbased%20contract.&text=Such%20contracts%20are%20then%20administered%20by%20UNDP.'
    local_file = '/data/sids/attribute_list_raw.csv'
    local_file = '/data/undp/zambia/zambia_energyaccess_poverty.csv'

    local_folder = '/data/undp/enmap/hrea/zarr/a/2'
    local_folder_cp = '/data/sids/'

    # example how to run using the class context wrapper

    async def example_run(sas_url=None):
        async with HandyContainerClient(sas_url=sas_url) as cc:
            await download_folder_from_azure(container_client_instance=cc, dst_folder=local_folder_cp,
                                             src_blob_name='rawdata/Raw GIS Data/Atlas/Data/ocean/gebco_2020_geotiff',
                                             strip_path=True)

    asyncio.run(example_run(sas_url=sas_url))

    # examples hot to run using a functional interface

    #cc = get_container_client(sas_url=write_sas_url)
    #asyncio.run(localfile2azureblob(container_client_instance=cc, src=local_file, overwrite=True))
    # asyncio.run(azureblob2localfile(container_client_instance=cc,blob_name='zambia_energyaccess_poverty.csv',dst_file='a'))

    #asyncio.run(folder2azureblob(container_client_instance=cc,src_folder=local_folder, dst_blob_name='ttt', overwrite=True, timeout=5))
    #asyncio.run(download_folder_from_azure(container_client_instance=cc, dst_folder=local_folder_cp, src_blob_name='ttt'))
