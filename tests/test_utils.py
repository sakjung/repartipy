import pytest

from repartipy.utils import extract_spark_version


@pytest.mark.parametrize(
    "spark_version_full_text,expected",
    [
        ("3.4.1", "3.4.1"),
        ("v3.4.1", "3.4.1"),
        ("3.4.1-amzn-0", "3.4.1"),
        ("alpha-0.1", "0.1"),
        ("3.4.0-rc1", "3.4.0"),
        ("v3.4.0-rc1", "3.4.0"),
    ],
)
def test_extract_spark_version(spark_version_full_text, expected):
    actual = extract_spark_version(spark_version_full_text=spark_version_full_text)
    assert actual == expected
