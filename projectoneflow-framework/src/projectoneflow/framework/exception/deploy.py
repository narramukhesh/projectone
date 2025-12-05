class DeployStrategyRunError(Exception):
    """This exception class will be raised if there is issue with deploy strategy run time error"""


class TerraformActionFetchError(Exception):
    """This exception class will be raised if there is any issue with terraform action fetch error"""


class TerrafromStatePushError(Exception):
    """This exception class will be raised if there is any issue with remote state push"""


class TerrafromStatePullError(Exception):
    """This exception class will be raised if there is any issue with remote state pull"""
