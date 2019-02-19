import logging
import os
import re

def logger(name):
    # Logger: create a console handler and set level to debug
    formatter = logging.Formatter('%(asctime)s %(levelname)8s:  %(message)s')
    handler   = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.addHandler(handler)

    level = os.environ['LOGGING_LEVEL'] if os.environ.get('LOGGING_LEVEL') else 'DEBUG'
    logger.setLevel(level)
    return logger

def queue_url_region(url):
    match = re.match(r'https://sqs.([^.]+)', url)
    if match:
        return match[1]
    raise Exception('Cannot extract AWS region from: %s' % url)

def enforce_env_vars(vars):
    for var in vars:
        if not os.environ.get(var):
            raise Exception('Environment variable not found: %s' % var)
