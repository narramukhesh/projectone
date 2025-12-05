from projectoneflow.core.schemas import ParentEnum
from projectoneflow.framework.ci.comment.gitlab import GitLabClient


class CIPlatformType(ParentEnum):
    """CICD Platform types supported"""

    gitlab = "gitlab"


def get_ci_client(ci_platform: CIPlatformType):
    if ci_platform == CIPlatformType.gitlab:
        return GitLabClient()
