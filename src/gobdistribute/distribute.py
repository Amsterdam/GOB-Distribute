import json
import logging
import os
import re
import requests
import tempfile
from gobconfig.datastore.config import get_datastore_config
from gobcore.datastore.factory import Datastore, DatastoreFactory
from gobcore.datastore.objectstore import ObjectDatastore, get_full_container_list, get_object
from gobcore.logging.logger import logger
from pathlib import Path
from typing import List, Tuple

from gobdistribute.config import CONTAINER_BASE, EXPORT_API_HOST, GOB_OBJECTSTORE
from gobdistribute.utils import json_loads

# Allow for variables in filenames. A variable will be converted into a regular expression
# and vice versa for a generated proposal
_REPLACEMENTS = {
    "{DATE}": r"\d{8}"
}

WILDCARD = "*"

logging.getLogger("paramiko").setLevel(logging.WARNING)


def distribute(catalogue, fileset=None):
    """
    Distribute export files for a given catalogue and optionally a collection

    :param catalogue: catalogue to distribute
    :param fileset: the fileset to distribute
    :return: None
    """
    distribute_info = f"Distribute catalogue {catalogue}"
    distribute_info += f" fileset {fileset}" if fileset else ""
    logger.info(distribute_info)

    logger.info("Connect to Objectstore")

    datastore, _ = _get_datastore(GOB_OBJECTSTORE)
    container_name = CONTAINER_BASE

    logger.info(f"Load files from {container_name}")
    conn_info = {
        "connection": datastore.connection,
        "container": container_name
    }

    # Get distribute configuration for the given catalogue, if a product is provided select only that product
    distribute_filesets = _get_config(conn_info, catalogue, container_name)
    export_products = _get_export_products(catalogue)

    logger.info("Disconnect from Objectstore")
    datastore.disconnect()

    filesets = {fileset: distribute_filesets.get(fileset)} if fileset else distribute_filesets

    for fileset, config in filesets.items():
        logger.info(f"Download fileset {fileset}")
        temp_fileset_dir = os.path.join(tempfile.gettempdir(), fileset)

        filenames = _get_filenames(conn_info, config, catalogue, export_products)
        src_files = _download_sources(conn_info, temp_fileset_dir, filenames)

        for destination in config.get('destinations', []):
            logger.info(f"Connect to Destination {destination['name']}")
            datastore, base_directory = _get_datastore(destination['name'])

            assert datastore.can_list_file() and datastore.can_delete_file(), \
                "Datastore does not support file deletions"

            dst_dir = f"{base_directory}{destination['location']}"

            logger.info(f"Distribute new files to Destination: {destination['name']}")
            logger.info(f"Distribute {len(src_files)} files to Location: {dst_dir}")

            _distribute_files(datastore, src_files, dst_dir)
            logger.info(f"Done distributing files to {destination['name']}")

            logger.info(f"Disconnect from Destination {destination['name']}")
            datastore.disconnect()


def _get_export_products(catalogue: str):
    """Retrieves the products overview from GOB-Export

    :return:
    """
    r = requests.get(f'{EXPORT_API_HOST}/products')
    r.raise_for_status()
    data = json.loads(r.text)
    return data.get(catalogue)


def _expand_filename_wildcard(conn_info: dict, filename: str):
    """Returns all filenames from the given container that match filename, taking into consideration the wildcard
    symbol.

    :param conn_info:
    :param filename:
    :return:
    """
    result = []
    match = filename.replace(WILDCARD, '.*')
    for item in get_full_container_list(conn_info['connection'], conn_info['container']):
        if re.match(match, item['name']) and item['content_type'] != 'application/directory':
            result.append(item['name'])
    return result


def _dst_path(source_file_path: str, base_dir: str):
    if not base_dir:
        return source_file_path

    return source_file_path.replace(base_dir, "")


def _get_filenames(conn_info: dict, config: dict, catalogue: str, export_products: dict) -> List[Tuple[str, str]]:
    """Determines filenames to download for sources in config.

    Source should have either 'file_name' or 'export' set. When source is 'file_name', this name is used. When source
    is 'export', the filenames to download are derived from the export products definition.

    Returns a list op 2-tuples (dst_path, source_filename), where dst_path is the relative location on the destination

    :param config:
    :param catalogue:
    :return:
    """
    # Download exports product definition
    filenames = []

    logger.info("Determining files from source to distribute")

    for source in config.get('sources', []):
        if source.get('file_name'):
            base_dir = source.get('base_dir', '')
            base_dir = f"{base_dir}/" if base_dir else base_dir
            base_dir = base_dir[:-1] if base_dir[-2:] == "//" else base_dir
            source_path = base_dir + source['file_name']

            if WILDCARD in source['file_name']:
                wildcard_files = _expand_filename_wildcard(conn_info, source_path)
                filenames.extend([(_dst_path(filename, base_dir), filename) for filename in wildcard_files])
                logger.info(f"Distribute files matching from source: {source_path}")
            else:
                filenames.append((_dst_path(source_path, base_dir), source_path))
                logger.info(f"Distribute file matching from source: {source_path}")

        elif source.get('export'):
            collection_config = export_products.get(source['export']['collection'], {})

            # If source['export']['products'] is defined, only take relevant products from collection_config.
            # If no products are defined, take all products from collection_config
            products = [collection_config.get(product, []) for product in source['export']['products']] \
                if source['export'].get('products') \
                else collection_config.values()

            # Flatten the list of lists (products)
            products = [item for product in products for item in product]

            for product in products:
                logger.info(f"Distribute files from source from export product set {source['export']['collection']} "
                            f"{product}")

            filenames += [(item, item) for item in [f'{catalogue}/{item}' for item in products]]

    return filenames


def _download_sources(conn_info, directory, filenames) -> List[Tuple[str, str]]:
    """

    :param conn_info:
    :param directory:
    :param filenames: list of tuples (dst_path, src_filename)
    :return:
    """
    path = Path(directory)
    path.mkdir(exist_ok=True)

    src_files = []

    for dst_path, filename in filenames:
        src_file_info, src_file = _get_file(conn_info, filename)

        temp_file = os.path.join(directory, dst_path)
        path = Path(os.path.dirname(temp_file))
        path.mkdir(exist_ok=True, parents=True)

        with open(temp_file, "wb") as f:
            f.write(src_file)
        src_files.append((dst_path, temp_file))

    logger.info(f"{len(src_files)} source files downloaded")

    return src_files


def _get_datastore(destination_name: str):
    """Returns Datastore and base_directory for Datastore.
    Returned Datastore has an initialised connection for destination_name

    :param destination_name:
    :return:
    """
    datastore_config = get_datastore_config(destination_name)
    datastore = DatastoreFactory.get_datastore(datastore_config)
    datastore.connect()

    # Prepend main directory to file, except for ObjectDatastore, as this will use a container by default
    base_directory = f"{CONTAINER_BASE}/" if not isinstance(datastore, ObjectDatastore) else ""
    return datastore, base_directory


def _apply_filename_replacements(filename: str):
    """Applies filename replacements to filename, if filename contains any patterns defined in _REPLACEMENTS.

    This is used to eliminate variables in filenames, such as timestamps.

    :param filename:
    :return:
    """
    for src, dst in _REPLACEMENTS.items():
        filename = re.sub(dst, src, filename)
    return filename


def _distribute_files(datastore: Datastore, mapping: List[tuple], dst_dir: str):
    """

    :param datastore:
    :param mapping: list of tuples containing (destination_path, local_path) pairs
    :param dst_dir: base dir to distribute fils to, prepended to destination_path to get to the full path
    :return:
    """
    distribute_files = {}

    # Prepare distribution. Determine existing files to delete
    for dst_path, local_file in mapping:
        destination = f'{dst_dir}/{dst_path}'
        fname_replaced = _apply_filename_replacements(destination)
        distribute_files[fname_replaced] = {
            'local_file': local_file,
            'destination': destination,
            'existing_files': [],
        }

    for f in datastore.list_files(dst_dir):
        fname_replaced = _apply_filename_replacements(f)
        if fname_replaced in distribute_files:
            distribute_files[fname_replaced]['existing_files'].append(f)

    for dist_file in distribute_files.values():
        _distribute_file(datastore, dist_file['local_file'], dist_file['destination'], dist_file['existing_files'])


def _distribute_file(datastore: Datastore, local_file: str, destination_filename: str, existing_files: List[str]):
    skip = False

    for f in existing_files:
        try:
            datastore.delete_file(f)
        except OSError:
            logger.error(f"Could not delete file {f}. Skipping distribution of {destination_filename}")
            skip = True
            break

    if not skip:
        datastore.put_file(local_file, destination_filename)


def _get_file(conn_info, filename):
    """
    Get a file from Objectstore
    Applies filename replacements, to find files with variables (such as timestamps) in their names.

    :param conn_info: Objectstore connection
    :param filename: name of the file to retrieve
    :return:
    """
    filename = _apply_filename_replacements(filename)

    obj_info = None
    obj = None
    for item in get_full_container_list(conn_info['connection'], conn_info['container']):
        item_name = _apply_filename_replacements(item['name'])

        if item_name == filename and (obj_info is None or item['last_modified'] > obj_info['last_modified']):
            # If multiple matches, match with the most recent item
            obj_info = dict(item)
            obj = get_object(conn_info['connection'], item, conn_info['container'])

    return obj_info, obj


def _get_config(conn_info, catalogue: str, environment: str):
    """
    Get test definitions for the given catalogue

    :param conn_info: Objectstore connection
    :param catalogue: Catalogue name
    :param environment: development, acceptatie, productie (equals container)
    :return:
    """
    filename = f"distribute.{environment}.{catalogue}.json"
    _, config_file = _get_file(conn_info, filename)
    if config_file is None:
        logger.error(f"Missing config file: {filename}")
        return {}
    try:
        return json_loads(config_file.decode("utf-8"))
    except json.JSONDecodeError as e:
        logger.error(f"JSON error in checks file '{filename}': {str(e)}")
        return {}
