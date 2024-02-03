from unittest import mock

import pytest


@pytest.fixture
def mocker() -> mock:
    return mock

