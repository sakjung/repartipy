import re


def extract_spark_version(spark_version_full_text: str) -> str:
    """Extract only version part from the full version text.

    :param spark_version_full_text: full string of spark version e.g. 3.4.1, 3.4.1-amzn-0
    :return: spark version string e.g. 3.4.1
    """
    return re.search(r"[.]*([\d.]+)[.]*", spark_version_full_text).group(1)
