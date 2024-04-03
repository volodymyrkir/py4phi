import pytest

from py4phi.analytics.base_analytics import Analytics


@pytest.fixture()
def analytics(mocker):
    mocker.patch.object(Analytics, '__abstractmethods__', set())
    return Analytics(mocker.MagicMock(), 'target')


def test_successful_init(analytics):
    assert analytics._target_column == 'target'
    assert analytics._df is not None
