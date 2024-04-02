import pytest

from py4phi.controller import Controller


@pytest.fixture()
def controller():
    return Controller(dataset_handler=None)
