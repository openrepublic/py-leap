import pytest
import logging


def test_performance(cleos_w_eosmechs):
    cleos = cleos_w_eosmechs
    result = cleos.push_action('eosmechanics', 'cpu', [], 'eosmechanics')
    logging.info(result['processed']['receipt'])
