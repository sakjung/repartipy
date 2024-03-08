from unittest import mock

import pytest
from _pytest.monkeypatch import MonkeyPatch

from repartipy.exceptions.exceptions import (DataFrameNotCachedException,
                                             NotFullyInitializedException)
from repartipy.size_estimator import SamplingSizeEstimator, SizeEstimator


class TestSizeEstimator:
    def test_cache_and_unpersist(self, spark, input_data):
        """
        Given normal DF
        When use SizeEstimator with context manager
        Then cache within the statement and unpersist outside the statment
        """
        assert not input_data.is_cached
        with SizeEstimator(spark=spark, df=input_data):
            assert input_data.is_cached
        assert not input_data.is_cached

    def test_estimate_normal_df(self, spark, input_data):
        """
        Given normal DF
        When estimate the size (bytes)
        Then the size is larger than 0
        """
        with SizeEstimator(spark=spark, df=input_data) as size_estimator:
            size_in_bytes = size_estimator.estimate()
            assert size_in_bytes > 0

    def test_estimate_empty_df(self, spark, empty_input_data):
        """
        Given empty DF
        When estimate the size (bytes)
        Then the size is equal to zero
        """
        with SizeEstimator(spark=spark, df=empty_input_data) as size_estimator:
            size_in_bytes = size_estimator.estimate()
            assert size_in_bytes == 0

    def test_get_desired_partition_count(self, spark, input_data):
        """
        Given desired partition size (default 1 GiB)
        When calculate partition count that can make each partition have lower than but close to the desired partition size
        Then get desired partition count
        """
        with SizeEstimator(spark=spark, df=input_data) as size_estimator:
            desired_partition_count = size_estimator.get_desired_partition_count()
            assert desired_partition_count == 1

    def test_init_without_context_manager(self, spark, input_data):
        """
        Given Init SizeEstimator
        When use without context manager
        Then estimate method throws exception
        """
        with pytest.raises(NotFullyInitializedException):
            SizeEstimator(spark=spark, df=input_data).estimate()

    def test__get_df_size_in_bytes_not_cached(self, spark, input_data):
        """
        Given non-cached DataFrame
        When call _get_df_size_in_bytes
        Then throws exception
        """
        with pytest.raises(DataFrameNotCachedException):
            se = SizeEstimator(spark=spark, df=input_data)
            se._get_df_size_in_bytes(dataframe=input_data)


class TestSamplingSizeEstimator:
    def setup_method(self):
        self.monkeypatch = MonkeyPatch()
        self.monkeypatch.setattr(
            "repartipy.size_estimator.SamplingSizeEstimator.__exit__", mock.Mock()
        )

    def test_persist_and_unpersist(self, spark, input_data, monkeypatch):
        """
        Given normal DF
        When use SamplingSizeEstimator with context manager
        Then persist (write) on HDFS within the statement and unpersist outside the statment
        """
        with SamplingSizeEstimator(spark=spark, df=input_data) as size_estimator:
            assert size_estimator.temp_path is not None

    def test_estimate_do_sampling(self, spark, input_data, monkeypatch):
        """
        Given normal DF with uneven partition size
        When estimate the size (bytes)
        Then the estimated df size differs to the actual df size
        """
        additional_input_data = input_data.limit(3)
        new_df = input_data.union(additional_input_data).repartition(2)
        monkeypatch.setattr(
            "repartipy.size_estimator.SamplingSizeEstimator.reproduce", mock.Mock(return_value=new_df)
        )

        new_df.cache().foreach(lambda x: x)
        # new_df size in bytes = 164 bytes
        actual_size_in_bytes = (
            spark._jsparkSession.sessionState()
            .executePlan(new_df._jdf.queryExecution().logical(), new_df._jdf.queryExecution().mode())
            .optimizedPlan()
            .stats()
            .sizeInBytes()
        )
        new_df.unpersist()

        with SamplingSizeEstimator(spark=spark, df=new_df, sample_count=1) as size_estimator:
            estimated_size_in_bytes = size_estimator.estimate()
            assert estimated_size_in_bytes != actual_size_in_bytes

    def test_estimate_normal_df(self, spark, input_data, monkeypatch):
        """
        Given normal DF
        When estimate the size (bytes)
        Then the size is larger than 0
        """
        monkeypatch.setattr(
            "repartipy.size_estimator.SamplingSizeEstimator.reproduce", mock.Mock(return_value=input_data)
        )

        with SamplingSizeEstimator(spark=spark, df=input_data) as size_estimator:
            size_in_bytes = size_estimator.estimate()
            assert size_in_bytes > 0

    def test_estimate_empty_df(self, spark, empty_input_data, monkeypatch):
        """
        Given empty DF
        When estimate the size (bytes)
        Then the size is equal to zero
        """
        monkeypatch.setattr(
            "repartipy.size_estimator.SamplingSizeEstimator.reproduce", mock.Mock(return_value=empty_input_data)
        )

        with SamplingSizeEstimator(spark=spark, df=empty_input_data) as size_estimator:
            size_in_bytes = size_estimator.estimate()
            assert size_in_bytes == 0

    def test_get_desired_partition_count(self, spark, input_data, monkeypatch):
        """
        Given desired partition size (default 1 GiB)
        When make each partition have lower than but close to the desired partition size
        Then get desired partition count
        """
        monkeypatch.setattr(
            "repartipy.size_estimator.SamplingSizeEstimator.reproduce", mock.Mock(return_value=input_data)
        )

        with SamplingSizeEstimator(spark=spark, df=input_data) as size_estimator:
            desired_partition_count = size_estimator.get_desired_partition_count()
            assert desired_partition_count == 1

    def test_init_without_context_manager(self, spark, input_data):
        """
        Given Init SizeEstimator
        When use without context manager
        Then estimate method throws exception
        """
        with pytest.raises(NotFullyInitializedException):
            SamplingSizeEstimator(spark=spark, df=input_data).estimate()

    def test__get_df_size_in_bytes_not_cached(self, spark, input_data):
        """
        Given non-cached DataFrame
        When call _get_df_size_in_bytes
        Then throws exception
        """
        with pytest.raises(DataFrameNotCachedException):
            se = SizeEstimator(spark=spark, df=input_data)
            se._get_df_size_in_bytes(dataframe=input_data)
