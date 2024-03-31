import os

from py4phi.utils import prepare_location


def test_prepare_location(tmp_path, mocker):
    test_location = tmp_path / "test_folder"
    test_location.mkdir()
    logger = mocker.patch("py4phi.utils.logger")

    prepare_location(test_location)

    assert logger.debug.called
    assert test_location.exists()
    assert os.listdir(test_location) == []
