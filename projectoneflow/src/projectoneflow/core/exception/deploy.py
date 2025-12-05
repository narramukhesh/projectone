class PipelineConfigurationError(Exception):
    """This Exception is used for raising any pipeline configuration errors"""


class PipelineTaskDependencyError(Exception):
    """This Exception is used for raising because of the pipeline dependency error"""


class PipelineTaskLibraryResolutionError(Exception):
    """This Exception is used for raising because of the task library resolution error"""


class SparkTaskConfigurationError(Exception):
    """This Exception is used for raising because of incorrect spark task configuration"""


class InvalidPipelineConfigurationError(Exception):
    """This Exception is used for raising because of incorrect of invalid Pipeline configuration"""


class TerraformBackendTypeNotSupported(Exception):
    """This Exception is used for raising because of incorrect backend type provided"""
