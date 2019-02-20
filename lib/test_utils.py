import pytest
import os
import utils


def test_queue_url_region():
    # Good URL
    assert utils.queue_url_region('https://sqs.us-east-1.1234567890/queue')

    # Unsafe URL
    with pytest.raises(AssertionError) as excinfo:
        utils.queue_url_region('http://sqs.us-east-1.1234567890/queue')
    assert str(excinfo.value) == 'Queue URL must have https scheme'

    # Wrong format
    with pytest.raises(AssertionError) as excinfo:
        utils.queue_url_region('https://foo.bar.us-east-1.1234567890/queue')
    assert str(excinfo.value) == 'Failed to extract AWS region from https://foo.bar.us-east-1.1234567890/queue'


def test_enforce_env_vars():
    # vars are present
    os.environ['FOOBLYA1'] = '13'
    os.environ['FOOBLYA2'] = '14'
    assert None == utils.enforce_env_vars(['FOOBLYA1', 'FOOBLYA2'])

    # vars are missing
    with pytest.raises(AssertionError) as excinfo:
        utils.enforce_env_vars(['FOOBLYA1', 'FOOBLYA2', 'FOOBLYA3'])
    assert str(excinfo.value) == 'Environment variable is not present: FOOBLYA3'
