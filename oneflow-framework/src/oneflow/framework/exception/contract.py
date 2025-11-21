class ContractNotDefined(Exception):
    """This Exception will be raised if any contract is invoked and that is not defined"""


class ProjectContractNotExists(Exception):
    """This Exception will be raised if any contract is invoked and that is not defined"""


class ProjectContractDatasetNotExists(Exception):
    """This Exception will be raised if project contract dataset if doesn't exists"""


class ProjectConfigValidationError(Exception):
    """This Exception will be raised if project contract has validation errors"""


class ProjectContractPipelineNotExists(Exception):
    """This Exception will be raised if project contract pipelines folder if doesn't exists"""


class ProjectArtifactCreationError(Exception):
    """This Exception will be raised if the artifact aren't able to created"""


class DatabricksCredentialsValidationError(Exception):
    """This Exception will be raised if the credentials provided aren't following some guidelines"""


class DataObjectPatternMismatch(Exception):
    """This Exception will be raised if the data object pattern mismatch with required pattern"""


class SelectedProjectObjectDoesnotExist(Exception):
    """This Exception will be raised if the selected object doesn't exists in provided object list"""


class DeployStrategyDoesnotExist(Exception):
    """This Exception will be raise if the selected environment doesn't support the deploy strategy"""


class EnvironmentParseError(Exception):
    """This Exception will be raised if the there is problem with parsing issues with environment variables"""


class DeployDetailsMissingError(Exception):
    """This Exception will be raised if there is problem with deploy server details missing from configuration"""
