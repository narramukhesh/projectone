class SparkSourceCDCInitializationError(Exception):
    """This Exception raised if any issue with cdc error"""


class FileSparkSourceCDCInitializationError(SparkSourceCDCInitializationError):
    """This Exception raised if any issue with file based cdc error"""


class DeltaSparkSourceCDCInitializationError(SparkSourceCDCInitializationError):
    """This Exception raised if any issue with delta source cdc error"""


class SourceModuleNotImplemented(Exception):
    """This Exception raised if any issue with the source not able to recognize"""


class WriteFunctionNotImplementedError(Exception):
    """This Exception raised if any Source did'nt support weite type"""


class ColumnTypeParsingError(Exception):
    """This Exception raised if any column parsing error faced if not able to parse to specific provider type"""


class SharepointRequestException(Exception):
    """This Exception raised if any issues with sharepoint parsing error faced if extraction failed"""


class SFTPRequestException(Exception):
    """This Exception raised if any issues with sftp request parsing error faced if extraction failed"""


class FileDataCompressionParseError(Exception):
    """This Exception raised if any issues with file which is compressed and not able to uncompress"""


class FileSourceCredentialsValidationError(Exception):
    """This Exception raised if any issues with file source required valiation issues"""


class NoSourceData(Exception):
    """This Exception raised if any issues with source has data"""


class SourceSchemaError(Exception):
    """This Exception raised if any issues with source schema provided by user"""
