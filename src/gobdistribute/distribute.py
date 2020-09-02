import json
import os
from pathlib import Path
import re
import tempfile

from objectstore.objectstore import get_full_container_list, get_object

from gobconfig.datastore.config import get_datastore_config

from gobcore.datastore.factory import DatastoreFactory
from gobcore.datastore.objectstore import ObjectDatastore
from gobcore.logging.logger import logger

from gobdistribute.config import CONTAINER_BASE, GOB_OBJECTSTORE
from gobdistribute.utils import json_loads


# Allow for variables in filenames. A variable will be converted into a regular expression
# and vice versa for a generated proposal
_REPLACEMENTS = {
    "{DATE}": r"\d{8}"
}


def distribute(catalogue, collection=None, product=None):
    """
    Distribute export files for a given catalogue and optionally a collection

    :param catalogue: catalogue to distribute
    :param collection: collection to distribute
    :return: None
    """
    distribute_info = f"Distribute catalogue {catalogue}"
    distribute_info += f" collection {collection}" if collection else ""
    distribute_info += f" product {product}" if product else ""
    logger.info(distribute_info)

    logger.info(f"Connect to Objectstore")

    config = get_datastore_config(GOB_OBJECTSTORE)
    datastore = DatastoreFactory.get_datastore(config)
    datastore.connect()
    container_name = CONTAINER_BASE

    logger.info(f"Load files from {container_name}")
    conn_info = {
        "connection": datastore.connection,
        "container": container_name
    }

    # Get distribute configuration for the given catalogue, if a product is provided select only that product
    distribute_products = _get_config(conn_info, catalogue)
    products = {product: distribute_products.get(product)} if product else distribute_products

    for product, config in products.items():
        logger.info(f"Download product {product}")
        temp_product_dir = os.path.join(tempfile.gettempdir(), product)

        # Create the path if the path not yet exists
        path = Path(temp_product_dir)
        path.mkdir(exist_ok=True)

        src_files = _download_sources(conn_info, temp_product_dir, config, collection)

        _distribute_files(config, src_files)

    return


def _download_sources(conn_info, directory, config, collection=None):
    src_files = []

    src_config = config['source']

    # Select the collection if provided or download all collections in the config
    collections = [collection] if collection else src_config.get('collections').keys()
    for collection in collections:
        col_file_name = f"{src_config['collections'][collection]['file_name']}"

        file_path = f"{src_config['location']}/{col_file_name}"
        src_file_info, src_file = _get_file(conn_info, file_path)

        # Store file in temporary directory
        temp_file = os.path.join(directory, col_file_name)
        with open(temp_file, "wb") as f:
            f.write(src_file)
        src_files.append(temp_file)

    logger.info(f"{len(src_files)} files downloaded")

    return src_files


def _distribute_files(config, files):
    for destination in config.get('destinations'):
        logger.info(f"Connect to Destination {destination['name']}")

        datastore_config = get_datastore_config(destination['name'])
        datastore = DatastoreFactory.get_datastore(datastore_config)
        datastore.connect()

        # Prepend main directory to file, except for ObjectDatastore, as this will use a container by default
        base_directory = f"{CONTAINER_BASE}/" if not isinstance(datastore, ObjectDatastore) else ""

        logger.info(f"Distribute {len(files)} files to Destination {destination['name']}")
        for file in files:
            base = os.path.basename(file)
            datastore.put_file(file, f"{base_directory}{destination['location']}/{base}")
            logger.info(f"{base} distributed to {destination['name']}")


def _get_file(conn_info, filename):
    """
    Get a file from Objectstore

    :param conn_info: Objectstore connection
    :param filename: name of the file to retrieve
    :return:
    """
    # If the filename contains any replacement patterns, use the pattern to find the file
    for src, dst in _REPLACEMENTS.items():
        filename = re.sub(dst, src, filename)

    obj_info = None
    obj = None
    for item in get_full_container_list(conn_info['connection'], conn_info['container']):
        item_name = item['name']
        for src, dst in _REPLACEMENTS.items():
            item_name = re.sub(dst, src, item_name)

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
