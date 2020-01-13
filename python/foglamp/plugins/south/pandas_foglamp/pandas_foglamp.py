# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Module for  reading csv data in chunks plugin """

import copy
import uuid
import logging
import os

from foglamp.common import logger
from foglamp.plugins.common import utils
try:
    import pandas as pd
    import numpy as np
except (Exception, RuntimeError) as ex:
    _LOGGER.exception("Pandas module not imported : {}".format(str(ex)))
    raise ex

__author__ = "Deepanshu Yadav"
__copyright__ = "Copyright (c) 2019 Dianomic Systems"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


_DEFAULT_CONFIG = {
    'plugin': {
        'description': 'Reads a CSV File in chunks and gives average  of an attribute of the dataframe',
        'type': 'string',
        'default': 'pandas_foglamp',
        'readonly': 'true'
    },
    'assetName': {
        'description': 'Name of Asset',
        'type': 'string',
        'default': 'pandas_foglamp',
        'displayName': 'Asset name'
    },
    'fileName':
    {
        'description':'Name Of the CSV FILE',
        'type':'string',
        'default': 'train.csv',
        'displayName':'Name of CSV FILE'

    },
    'filePath':
    {
        'description':'Loacation Of the CSV FILE',
        'type':'string',
        'default': '/home/foglamp/deepanshu',
        'displayName':'Path of CSV FILE'

    },
    'attributeName':
    {
        'description':'The column whose values have to be returned from the  CSV FILE',
        'type':'string',
        'default': 'Units Sold',
        'displayName':'The column  of the csv file '

    },
    'chunkSize':
    {
        'description':'chunk size',
        'type':'string',
        'default': '10000',
        'displayName':'CHUNK SIZE'

    }
}




_LOGGER = logger.setup(__name__, level=logging.INFO)
FILE_PATH = None
FILE_NAME = None
CHUNK_SIZE = None
df = None
ATTRIBUTE_NAME = None


def generate_data(attribute_name):
    global df
    try:
        # get the next item
        element = next(df)
        _LOGGER.info('element is calculated')
        if element is not None:
            val = element[attribute_name].values
            return val.mean() , np.median( val  ) , np.sqrt( np.mean( val**2  )  )
        else:
            _LOGGER.exception( 'element calculated is null so sending default value'  )
            return 1 , 2 , 3
    except StopIteration:
        # if StopIteration is raised, read the file again
        _LOGGER.info('stop iteration')
        df = iter(pd.read_csv(FILE_NAME , chunksize= CHUNK_SIZE   ) )
        element = next( df )
        val = element[attribute_name].values
        return val.mean() , np.median(val ) , np.sqrt( np.mean( val**2  )  )


def plugin_info():
    """ Returns information about the plugin.
    Args:
    Returns:
        dict: plugin information
    Raises:
    """
    return {
        'name': 'Pandas CSV Reader',
        'version': '1.7.0',
        'mode': 'poll',
        'type': 'south',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }

def plugin_init(config):
    """ Initialise the plugin.
    Args:
        config: JSON configuration document for the South plugin configuration category
    Returns:
        data: JSON object to be used in future calls to the plugin
    Raises:
    """

    global df
    global FILE_PATH
    global FILE_NAME
    global CHUNK_SIZE
    global ATTRIBUTE_NAME

    _LOGGER.info("config dictionary {} \n ".format(config))
    try:
        _LOGGER.info("config type {} \n ".format(type(config['fileName']['value'])  ))
        file_name = config['fileName']['value'] 

        FILE_PATH = config['filePath']['value']
        FILE_NAME = os.path.join( FILE_PATH , file_name )
        CHUNK_SIZE = int(config['chunkSize']['value'] )
        ATTRIBUTE_NAME = config['attributeName']['value']
        #_LOGGER.exception("INSIDE....")
        df = iter(pd.read_csv(FILE_NAME , chunksize= CHUNK_SIZE))
        #_LOGGER.exception("NIKAL GYA...")
    except (Exception, RuntimeError) as ex:
        _LOGGER.exception("OUCH Something wrong with file reading : {}".format(str(ex)))
        raise ex
    data = copy.deepcopy(config)
    return data


def plugin_poll(handle):
    """ Extracts data from the sensor and returns it in a JSON document as a Python dict.
    Available for poll mode only.
    Args:
        handle: handle returned by the plugin initialisation call
    Returns:
        returns a sensor reading in a JSON document, as a Python dict, if it is available
        None - If no reading is available
    Raises:
        Exception
    """
    try:
        _LOGGER.info('Arrived at poll function')
        time_stamp = utils.local_timestamp()
        mean_value , median_value , rms_value = generate_data(ATTRIBUTE_NAME)
        data = {'asset':  handle['assetName']['value'], 'timestamp': time_stamp, 'key': str(uuid.uuid4()), 'readings': {"mean_value": mean_value , "median_value":median_value , "rms_value":rms_value  }}
    except (Exception, RuntimeError) as ex:
        _LOGGER.exception("Pandas CSV Reader exception: {}".format(str(ex)))
        raise ex
    else:
        return data


def plugin_reconfigure(handle, new_config):
    """ Reconfigures the plugin

    Args:
        handle: handle returned by the plugin initialisation call
        new_config: JSON object representing the new configuration category for the category
    Returns:
        new_handle: new handle to be used in the future calls
    """
    _LOGGER.info("Old config for sinusoid plugin {} \n new config {}".format(handle, new_config))
    new_handle = copy.deepcopy(new_config)
    return new_handle


def plugin_shutdown(handle):
    """ Shutdowns the plugin doing required cleanup, to be called prior to the South plugin service being shut down.

    Args:
        handle: handle returned by the plugin initialisation call
    Returns:
        plugin shutdown
    """
    _LOGGER.info('Pandas Plugin shut down.')

