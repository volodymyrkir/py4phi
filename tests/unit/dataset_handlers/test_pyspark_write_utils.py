"""Contains tests for the pyspark writing utility functions."""
import os

import pytest
from py4j.java_gateway import JavaObject, JavaPackage

from py4phi.dataset_handlers.pyspark_write_utils import (
    configure_hadoop,
    copy_merge_into,
    delete_location,
    ensure_exists,
    get_files,
)


def test_configure_hadoop(spark_session):
    hadoop, conf, fs = configure_hadoop(spark_session)
    assert isinstance(hadoop, JavaPackage)
    assert isinstance(conf, JavaObject)
    assert isinstance(fs, JavaObject)


def test_ensure_exists(spark_session, tmp_path):
    ensure_exists(spark_session, str(tmp_path))
    assert os.path.exists(str(tmp_path))


def test_delete_location(spark_session, tmp_path):
    delete_location(spark_session, str(tmp_path))
    assert not os.path.exists(str(tmp_path))


def test_get_files_empty(spark_session, tmp_path):
    with pytest.raises(ValueError):
        get_files(spark_session, str(tmp_path))


def test_get_files(spark_session, tmp_path):
    with open(tmp_path / 'file.txt', 'w'):
        files = get_files(spark_session, str(tmp_path))
        assert len(files) == 1
        assert isinstance(files[0], JavaObject)


@pytest.mark.parametrize('delete_source', [True, False])
def test_copy_merge_into(spark_session, tmp_path_factory, delete_source):
    tmp_path = tmp_path_factory.mktemp('files1')
    tmp_path2 = tmp_path_factory.mktemp('files2')
    for file in ['file1.txt', 'file2.txt']:
        f = open(tmp_path / file, 'w')
        f.write('hello')
    copy_merge_into(
        spark_session,
        str(tmp_path),
        str(tmp_path2 / 'res_file.csv'),
        delete_source
    )
    exist_condition_met = (
        not os.path.exists(tmp_path)
        if delete_source else os.path.exists(tmp_path)
    )
    assert exist_condition_met
    assert os.path.exists(str(tmp_path2 / 'res_file.csv'))
