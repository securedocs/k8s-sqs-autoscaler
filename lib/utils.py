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
    match = re.match(r'(https?)://sqs.([^.]+)', url)
    assert match, 'Failed to extract AWS region from %s' % url

    scheme, region = match[1], match[2]
    assert scheme == 'https', 'Queue URL must have https scheme'

    return region

def enforce_env_vars(vars):
    for var in vars:
        assert os.environ.get(var), 'Environment variable is not present: %s' % var
