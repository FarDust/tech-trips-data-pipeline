from enum import Enum


class ProtocolType(str, Enum):
    GCS = "gcs://"
    LOCAL = "file://"


DEFAULT_PROTOCOL = ProtocolType.LOCAL
URI_EQUAL_SIGN = "%3D"

# This chunk size is based on https://jbrojbrojbro.medium.com/finding-the-optimal-download-size-with-gcs-259dc7f26ad2
# This ensures 1MB chunks on the current data
CSV_CHUNK_SIZE = 25000
