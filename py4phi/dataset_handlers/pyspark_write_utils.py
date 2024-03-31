"""PySpark writing utilities."""
from typing import List

from py4j.java_gateway import JavaObject
from pyspark.sql import SparkSession


def configure_hadoop(spark: SparkSession) -> tuple:
    """
    Configure Hadoop.

    Args:
    ----
        spark: SparkSession object

    Returns: hadoop, hadoop conf, hadoop FS

    """
    hadoop = spark.sparkContext._jvm.org.apache.hadoop
    conf = hadoop.conf.Configuration()
    fs = hadoop.fs.FileSystem.get(conf)
    return hadoop, conf, fs


def ensure_exists(spark: SparkSession, file: str) -> None:
    """
    Ensure file exists with hadoop API, create sub folders.

    Args:
    ----
    spark (SparkSession): SparkSession object.
    file (str): file path.

    Returns: None

    """
    hadoop, _, fs = configure_hadoop(spark)
    if not fs.exists(hadoop.fs.Path(file)):
        fs.create(hadoop.fs.Path(file)).close()


def delete_location(spark: SparkSession, location: str) -> None:
    """
    Delete location by path.

    Args:
    ----
    spark (SparkSession): SparkSession object.
    location (str): location path to be deleted.

    Returns: None

    """
    hadoop, _, fs = configure_hadoop(spark)
    if fs.exists(hadoop.fs.Path(location)):
        fs.delete(hadoop.fs.Path(location), True)


def get_files(spark: SparkSession, src_dir: str) -> List[JavaObject]:
    """
    Get list of files in HDFS directory.

    Args:
    ----
    spark (SparkSession): SparkSession object.
    src_dir (str): source directory path.

    Returns: List of JavaObjects (files).

    """
    hadoop, _, fs = configure_hadoop(spark)
    ensure_exists(spark, src_dir)
    files = []
    for f in fs.listStatus(hadoop.fs.Path(src_dir)):
        if f.isFile():
            files.append(f.getPath())
    if not files:
        raise ValueError("Source directory {} is empty".format(src_dir))

    return files


def copy_merge_into(
    spark: SparkSession, src_dir: str, dst_file: str, delete_source: bool = True
) -> None:
    """
    Merge files from HDFS source directory into single destination file.

    Args:
    ----
    spark: SparkSession
    src_dir: path to the directory where dataframe was saved in multiple parts
    dst_file: path to single file to merge the src_dir contents into
    delete_source: flag for deleting src_dir and contents after merging

    Returns: None.

    """
    hadoop, conf, fs = configure_hadoop(spark)

    files = get_files(spark, src_dir)

    if fs.exists(hadoop.fs.Path(dst_file)):
        tmp_dst_file = dst_file + ".tmp"
        tmp_in_stream = fs.open(hadoop.fs.Path(dst_file))
        tmp_out_stream = fs.create(hadoop.fs.Path(tmp_dst_file), True)
        try:
            hadoop.io.IOUtils.copyBytes(
                tmp_in_stream, tmp_out_stream, conf, False
            )
        finally:
            tmp_in_stream.close()
            tmp_out_stream.close()

        tmp_in_stream = fs.open(hadoop.fs.Path(tmp_dst_file))
        out_stream = fs.create(hadoop.fs.Path(dst_file), True)
        try:
            hadoop.io.IOUtils.copyBytes(tmp_in_stream, out_stream, conf, False)
        finally:
            tmp_in_stream.close()
            fs.delete(hadoop.fs.Path(tmp_dst_file), False)
    else:
        out_stream = fs.create(hadoop.fs.Path(dst_file), False)

    try:
        for file in files:
            in_stream = fs.open(file)
            try:
                hadoop.io.IOUtils.copyBytes(
                    in_stream, out_stream, conf, False
                )
            finally:
                in_stream.close()
    finally:
        out_stream.close()

    if delete_source:
        delete_location(spark, src_dir)
