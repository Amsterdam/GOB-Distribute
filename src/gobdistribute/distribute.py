import json
import os
from pathlib import Path
import re
import tempfile
import requests

from typing import List

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
    distribute_filesets = _get_config(conn_info, catalogue)
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

            # Mapping is a list of tuples (local_file, destination_path)
            mapping = [
                (file, f"{dst_dir}/{os.path.basename(file)}") for file in src_files
            ]

            logger.info(f"Remove old files from Destination {destination['name']}")
            _delete_old_files(datastore, dst_dir, mapping)

            logger.info(f"Distribute {len(mapping)} files to Destination {destination['name']}")
            _distribute_files(datastore, mapping)
            logger.info(f"Done distributing files to {destination['name']}")


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
        if re.match(match, item['name']):
            result.append(item['name'])
    return result


def _get_filenames(conn_info: dict, config: dict, catalogue: str):
    """Determines filenames to download for sources in config.

    Source should have either 'file_name' or 'export' set. When source is 'file_name', this name is used. When source
    is 'export', the filenames to download are derived from the export products definition.

    :param config:
    :param catalogue:
    :return:
    """
    # Download exports product definition
    export_products = _get_export_products(catalogue)
    filenames = []

    for source in config.get('sources', []):
        if source.get('file_name'):
            if WILDCARD in source['file_name']:
                filenames.extend(_expand_filename_wildcard(conn_info, source['file_name']))
            else:
                filenames.append(source['file_name'])

        elif source.get('export'):
            collection_config = export_products.get(source['export']['collection'], {})

            # If source['export']['products'] is defined, only take relevant products from collection_config.
            # If no products are defined, take all products from collection_config
            products = [collection_config.get(product, []) for product in source['export']['products']] \
                if source['export'].get('products') \
                else collection_config.values()

            # Flatten the list of lists (products)
            filenames += [f'{catalogue}/{item}' for product in products for item in product]

    return filenames


def _download_sources(conn_info, directory, filenames):
    path = Path(directory)
    path.mkdir(exist_ok=True)

    src_files = []

    for filename in filenames:
        src_file_info, src_file = _get_file(conn_info, filename)

        # Store file in temporary directory. Use src_file_info['name'] as name, as the filename found can differ from
        # the exact filename we were looking for.
        temp_file = os.path.join(directory, os.path.basename(src_file_info['name']))
        with open(temp_file, "wb") as f:
            f.write(src_file)
        src_files.append(temp_file)

    logger.info(f"{len(src_files)} files downloaded")

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
    :param filemapping:
    :return:
    """

    if not datastore.can_list_file() or not datastore.can_delete_file():
        logger.warning(f"Can not delete old files from Destination. Datastore does not support deletions.")
        return

    # Apply replacements to all dst filenames
    dst_files = [_apply_filename_replacements(t[1]) for t in filemapping]

    for f in datastore.list_files(location):
        if _apply_filename_replacements(f) in dst_files:
            datastore.delete_file(f)


def _distribute_files(datastore: Datastore, mapping: List[tuple]):
    """

    :param datastore:
    :param mapping:
    :return:
    """
    for local_file, dst_path in mapping:
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


def _get_config(conn_info, catalogue):
    """
    Get test definitions for the given catalogue

    :param conn_info: Objectstore connection
    :param catalogue: Catalogue name
    :return:
    """
    filename = f"distribute.{catalogue}.json"
    _, config_file = _get_file(conn_info, filename)
    if config_file is None:
        logger.error(f"Missing config file: {filename}")
        return {}
    try:
        return json_loads(config_file.decode("utf-8"))
    except json.JSONDecodeError as e:
        logger.error(f"JSON error in checks file '{filename}': {str(e)}")
        return {}
