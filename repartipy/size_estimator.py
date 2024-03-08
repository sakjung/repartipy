from __future__ import annotations

import logging
import math
import tempfile
import uuid
from abc import abstractmethod
from contextlib import AbstractContextManager
from random import sample
from types import TracebackType
from typing import TYPE_CHECKING, Optional

from packaging import version
from typing_extensions import Self

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

from repartipy.exceptions.exceptions import (DataFrameNotCachedException,
                                             NotFullyInitializedException)
from repartipy.utils import extract_spark_version

logger = logging.getLogger("repartipy")


class AbstractSizeEstimator(AbstractContextManager):
    """[Abstract Class] Base SizeEstimator."""

    MINIMUM_NUMBER_OF_PARTITION = 1
    KILOBYTES = 1024
    # 1GiB
    DESIRED_PARTITION_SIZE_IN_BYTES = 1073741824

    def __init__(self, spark: SparkSession, df: DataFrame) -> None:
        self.spark = spark
        self.df = df

    @abstractmethod
    def __enter__(self) -> Self:
        """Open the source for DataFrame reproduction."""

    @abstractmethod
    def __exit__(
        self,
        __exc_type: Optional[type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> None:
        """Close the source for DataFrame reproduction."""

    @abstractmethod
    def reproduce(self) -> DataFrame:
        """Reproduce a DataFrame.

        :return: input DataFrame
        """

    @abstractmethod
    def estimate(self) -> int:
        """Estimate the size of a DataFrame.

        :return: size of input DataFrame
        """

    def get_desired_partition_count(
        self, desired_partition_size_in_bytes: int = DESIRED_PARTITION_SIZE_IN_BYTES
    ) -> int:
        """Calculate the ideal number of partitions based on the `desired_partition_size_in_bytes` and the `DataFrame size`.

        :param desired_partition_size_in_bytes: desired size that a single partition should occupy in a DataFrame
        :return: partition count a DataFrame should have to make each partition have `desired_partition_size_in_bytes`
        """
        df_size_in_bytes = self.estimate()
        if df_size_in_bytes < desired_partition_size_in_bytes:
            return self.MINIMUM_NUMBER_OF_PARTITION
        return math.ceil(df_size_in_bytes / desired_partition_size_in_bytes)

    def _get_df_size_in_bytes(self, dataframe: DataFrame) -> int:
        """Calculate the size of given (cached) DataFrame.
        Since Spark v3.2.0, SessionState's executePlan() requires additional 'mode' parameter.

        :param dataframe: target cached DataFrame
        :return: size of the target DataFrame
        """
        if not dataframe.is_cached:
            raise DataFrameNotCachedException

        args = [dataframe._jdf.queryExecution().logical()]
        spark_version = extract_spark_version(
            spark_version_full_text=self.spark.version
        )
        if version.parse(spark_version) >= version.parse("3.2.0"):
            args.append(dataframe._jdf.queryExecution().mode())
        return (
            self.spark._jsparkSession.sessionState()
            .executePlan(*args)
            .optimizedPlan()
            .stats()
            .sizeInBytes()
        )


class SizeEstimator(AbstractSizeEstimator):
    """SizeEstimator using In-Memory Cache as the source of DataFrame reproduction (i.e. read DataFrame from cache).

    Use this when your executor resource (memory) is affordable to cache the whole DataFrame
    """

    def __init__(self, spark: SparkSession, df: DataFrame) -> None:
        """Init SizeEstimator.

        :param spark: spark session
        :param df: input DataFrame
        """
        super().__init__(spark, df)

    def __enter__(self) -> Self:
        """Persist DataFrame to cache.

        :return: instance of SizeEstimator with Cache source
        """
        if not self.df.is_cached:
            self.df.cache()
            logger.debug("Given DataFrame has been cached!")
        else:
            logger.warning(
                "Given DataFrame has already been cached. It will be un-persisted at the end of this "
                "SizeEstimator's lifecycle. This may lead to unexpected behavior of your application"
            )
        self.df.foreach(lambda x: x)
        return self

    def __exit__(
        self,
        __exc_type: Optional[type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> None:
        """Unpersist DataFrame from cache.

        :param __exc_type: exception type
        :param __exc_value: exception
        :param __traceback: traceback
        """
        if self.df.is_cached:
            self.df.unpersist()
            logger.debug("Given DataFrame has been un-persisted from cache!")

    def estimate(self) -> int:
        """Estimate the size of a DataFrame using Spark execution plan statistics.

        :return: size of input DataFrame
        """
        return self._get_df_size_in_bytes(dataframe=self.reproduce())

    def reproduce(self) -> DataFrame:
        """Reproduce (i.e. read from Cache) a DataFrame,
        in order to prevent possibly less performant reading from the origin source (e.g. S3).

        :return: input DataFrame
        """
        if not self.df.is_cached:
            raise NotFullyInitializedException(this=self)
        return self.df


class SamplingSizeEstimator(AbstractSizeEstimator):
    """SizeEstimator using HDFS as the source of DataFrame reproduction (i.e. read DataFrame from HDFS).

    Use this when your executor resource (memory) is NOT affordable to cache the whole dataframe.
    Unlike SizeEstimator, this use sampling method in order to estimate the whole DataFrame size.
    """

    PARTITION_ID_NAME = "pid"
    PATH_IDENTIFIER = "repartipy"
    DEFAULT_SAMPLE_COUNT = 10

    def __init__(
        self,
        spark: SparkSession,
        df: DataFrame,
        sample_count: int = DEFAULT_SAMPLE_COUNT,
    ) -> None:
        """Init SamplingSizeEstimator.

        :param spark: spark session
        :param df: input DataFrame
        :param sample_count: number of samples to apply when estimating DataFrame size
        """
        super().__init__(spark, df)
        self.sample_count = sample_count
        self.fs = None
        self.Path = None
        self.temp_path = None

    def __enter__(self) -> Self:
        """Persist DataFrame to HDFS.

        :return: instance of SamplingSizeEstimator with HDFS source
        """
        from pathlib import Path

        sc = self.spark.sparkContext
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
        self.fs = FileSystem.get(Configuration())
        self.Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        self.temp_path = str(
            Path(tempfile.mkdtemp()) / self.PATH_IDENTIFIER / str(uuid.uuid4())
        )
        self.df.write.mode("overwrite").parquet(self.temp_path)
        logger.debug("Given DataFrame has been persisted in HDFS!")

        return self

    def __exit__(
        self,
        __exc_type: Optional[type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> None:
        """Unpersist DataFrame from HDFS.

        :param __exc_type: exception type
        :param __exc_value: exception
        :param __traceback: traceback
        """
        if self.fs and self.Path and self.fs.exists(self.Path(self.temp_path)):
            self.fs.delete(self.Path(self.temp_path))
            logger.debug("Given DataFrame has been un-persisted from HDFS!")

    def estimate(self) -> int:
        """Estimate the size of a DataFrame using Spark execution plan statistics.
        This samples the partitions with sample_count (default 10), so the result might be less accurate than SizeEstimator's.

        :return: size of input DataFrame
        """
        from pyspark.sql.functions import col, spark_partition_id

        dataframe = self.reproduce()
        current_partition_count = dataframe.rdd.getNumPartitions()
        sample_partition_ids = sample(
            [*range(current_partition_count)],
            min(self.sample_count, current_partition_count),
        )
        sample_dataframe = (
            dataframe.withColumn(self.PARTITION_ID_NAME, spark_partition_id())
            .filter(col(self.PARTITION_ID_NAME).isin(sample_partition_ids))
            .drop(self.PARTITION_ID_NAME)
        )

        sample_dataframe.cache().foreach(lambda x: x)
        sample_df_size_in_bytes = self._get_df_size_in_bytes(dataframe=sample_dataframe)
        sample_dataframe.unpersist()

        average_partition_size_in_bytes = sample_df_size_in_bytes / len(
            sample_partition_ids
        )
        return round(average_partition_size_in_bytes * current_partition_count)

    def reproduce(self) -> DataFrame:
        """Reproduce (i.e. read from HDFS) a DataFrame,
        in order to prevent possibly less performant reading from the origin source (e.g. S3).

        :return: input DataFrame
        """
        if not self.temp_path:
            raise NotFullyInitializedException(this=self)
        return self.spark.read.parquet(self.temp_path)
