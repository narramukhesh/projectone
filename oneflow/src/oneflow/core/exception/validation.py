class SparkInputValidationError(Exception):
    """This Exception will raised when any spark task input validation error"""


class SparkOutputValidationError(Exception):
    """This Exception will raised when any spark task output validation error"""


class SparkTaskValidationError(Exception):
    """This Exception will raised when any spark task validation error"""


class SparkTaskResultError(Exception):
    """This Exception will be raised for task validation/incorrect error"""


class CreateTableValidationError(Exception):
    """This Exception will be raised for create table validation"""


class SparkTaskExecutionFunctionInitializationError(Exception):
    """This Exception will be raised if any issues with spark task execution function initialization"""


class SparkTaskInputInitializationError(Exception):
    """This Exception will be raised if any issues with spark task input initialization"""


class SparkTaskOuputInitializationError(Exception):
    """This Exception will be raised if any issues with spark task output initialization"""


class SchemaInferenceFromRegistryError(Exception):
    """This Exception will be raised if any issues with the schema registry schema inference"""


class SecretManagerInitializationError(Exception):
    """This Exception will be raised if any issues with the secret manager initialization"""


class CreateDataObjectValidationError(Exception):
    """This Exception will be raised if any issues with the creatio of data object validation error"""


class ChangeDataFeatureTypeParseError(Exception):
    """This Exception will be raised if any issues with the change data capture feature parse error"""


class IncorrectChangeFeatureValueType(Exception):
    """This Exception will be raised if any issues with the change data capture feature value type parse error"""


class IncorrectChangeDataCaptureStateValue(Exception):
    """This Exception will be raised if any issues with the change data capture feature state value type parse error"""
