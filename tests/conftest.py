import pytest

from leap.cleos import CLEOS
from leap.fixtures import bootstrap_test_nodeos


@pytest.fixture()
def nodeless_cleos():
    yield CLEOS()


@pytest.fixture(scope='module')
def cleos_w_bootstrap(request, tmp_path_factory):
    request.applymarker(pytest.mark.bootstrap(True))
    request.applymarker(pytest.mark.randomize(False))
    with bootstrap_test_nodeos(request, tmp_path_factory) as cleos:
        yield cleos


@pytest.fixture(scope='module')
def cleos_w_testcontract(request, tmp_path_factory):
    deploy_marker = pytest.mark.contracts(
        testcontract='tests/contracts/testcontract/v1')

    request.applymarker(deploy_marker)

    with bootstrap_test_nodeos(request, tmp_path_factory) as cleos:
        yield cleos


@pytest.fixture(scope='module')
def cleos_w_eosmechs(request, tmp_path_factory):
    deploy_marker = pytest.mark.contracts(
        eosmechanics='tests/contracts/eosmechanics')

    request.applymarker(deploy_marker)

    with bootstrap_test_nodeos(request, tmp_path_factory) as cleos:
        yield cleos


@pytest.fixture(scope='module')
def cleos_w_indextest(request, tmp_path_factory):
    deploy_marker = pytest.mark.contracts(
        cindextest='tests/contracts/cindextest',
        rindextest='tests/contracts/rindextest')

    random_marker = pytest.mark.randomize(False)

    request.applymarker(random_marker)
    request.applymarker(deploy_marker)

    with bootstrap_test_nodeos(request, tmp_path_factory) as cleos:
        yield cleos
