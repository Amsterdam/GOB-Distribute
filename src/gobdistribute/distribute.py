import json
import os
from pathlib import Path
import re
import tempfile
import requests
import logging

from typing import List, Tuple

from objectstore.objectstore import get_full_container_list, get_object

from gobconfig.datastore.config import get_datastore_config

from gobcore.datastore.factory import DatastoreFactory, Datastore
from gobcore.datastore.objectstore import ObjectDatastore
from gobcore.logging.logger import logger

from gobdistribute.config import CONTAINER_BASE, GOB_OBJECTSTORE, EXPORT_API_HOST
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

    logger.info(f"Connect to Objectstore")

    datastore, _ = _get_datastore(GOB_OBJECTSTORE)
    container_name = CONTAINER_BASE

    logger.info(f"Load files from {container_name}")
    conn_info = {
        "connection": datastore.connection,
        "container": container_name
    }

    # Get distribute configuration for the given catalogue, if a product is provided select only that product
    distribute_filesets = _get_config(conn_info, catalogue, container_name)

    logger.info(f"Disconnect from Objectstore")
    datastore.disconnect()

    filesets = {fileset: distribute_filesets.get(fileset)} if fileset else distribute_filesets

    for fileset, config in filesets.items():
        logger.info(f"Download fileset {fileset}")
        temp_fileset_dir = os.path.join(tempfile.gettempdir(), fileset)

        filenames = _get_filenames(conn_info, config, catalogue)
        src_files = _download_sources(conn_info, temp_fileset_dir, filenames)

        for destination in config.get('destinations', []):
            logger.info(f"Connect to Destination {destination['name']}")
            datastore, base_directory = _get_datastore(destination['name'])
            dst_dir = f"{base_directory}{destination['location']}"

            # Mapping is a list of tuples (destination_path, local_file)
            mapping = [
                (f"{dst_dir}/{dst_path}", local_file) for dst_path, local_file in src_files
            ]

            logger.info(f"Remove old files from Destination {destination['name']}")
            _delete_old_files(datastore, dst_dir, mapping)

            logger.info(f"Distribute new files to Destination: {destination['name']}")
            logger.info(f"Distribute {len(mapping)} files to Location: {dst_dir}")

            _distribute_files(datastore, mapping)
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


def _get_filenames(conn_info: dict, config: dict, catalogue: str) -> List[Tuple[str, str]]:
    """Determines filenames to download for sources in config.

    Source should have either 'file_name' or 'export' set. When source is 'file_name', this name is used. When source
    is 'export', the filenames to download are derived from the export products definition.

    Returns a list op 2-tuples (dst_path, source_filename), where dst_path is the relative location on the destination

    :param config:
    :param catalogue:
    :return:
    """
    # Download exports product definition
    export_products = _get_export_products(catalogue)
    filenames = []

    logger.info("Determining files from source to distribute")

    for source in config.get('sources', []):
        if source.get('file_name'):
            base_dir = source.get('base_dir', '')
            base_dir = f"{base_dir}/" if base_dir else base_dir
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


def _delete_old_files(datastore: Datastore, location: str, filemapping: List[tuple]):
    """Deletes all files present that match the destination filenames, taking into consideration the filename
    replacements

    :param datastore:
    :param location:
    :param filemapping: list of tuples (dst_path, local_file_path)
    :return:
    """

    if not datastore.can_list_file() or not datastore.can_delete_file():
        logger.warning(f"Can not delete old files from Destination. Datastore does not support deletions.")
        return

    # Apply replacements to all dst filenames
    dst_files = [_apply_filename_replacements(t[0]) for t in filemapping]

    for f in datastore.list_files(location):
        if _apply_filename_replacements(f) in dst_files:
            datastore.delete_file(f)


def _distribute_files(datastore: Datastore, mapping: List[tuple]):
    """

    :param datastore:
    :param mapping: list of tuples (dst_path, local_file_path)
    :return:
    """
    for dst_path, local_file in mapping:
        datastore.put_file(local_file, dst_path)


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
