class ExecutionFuncInitializeError(Exception):
    """This Exception will be raised if any execution function initializing error"""


class SparkTaskExecutionFunctionArgsMismatch(Exception):
    """This Exception will be raised if any execution function Arguments mismatch error"""


class WriteTableConstraintError(Exception):
    """This Exception will be raised if any table constraints mismatched with target source"""


class WriteTypeAttributesError(Exception):
    """This Exception will be raised if any attributes missing from source"""


class SparkTaskExecutionError(Exception):
    """This Exception will be raised if any issue with spark task execution"""


class SparkTaskInputExecutionError(Exception):
    """This Exception will be raised if any issue spark task input error"""


class SparkTaskOutputExecutionError(Exception):
    """This Exception will be raised if any issue with spark task output error"""


class SparkTaskExecutionFunctionError(Exception):
    """This Exception will be raised if any issue with spark task execution function error"""


class SecretManagerFetchFailedError(Exception):
    """This Exception will be raised if any issues with the secret manager Fetching key error"""


class SparkTaskCreationError(Exception):
    """This Exception will be raised if any issues with the spark task creation"""


class SparkTaskSuccessExecutionError(Exception):
    """This Exception will be raised if any issues occured while spark task executing and operator has specific condition to sucess not fail option"""
