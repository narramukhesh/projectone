import requests
from projectoneflow.framework.exception.ci import GitLabRequestException


class GitLabClient:
    BASE_URL = "https://gitlab.com/api/v4"

    def post_mr_comment(
        self, project_id, mr_id, body, private_token=None, project_token=None
    ):
        """
        This is the method to post the Merge Request Comments

        Parameters
        -----------------
        project_id:str
            Project id of the gitlab
        mr_id:str
            Merge request if for the gitlab
        body:str
            Body to be passed as a comment to gitlab
        private_token:str
            private token to be passed to gitlab
        project_token:str
            project token to be passed to gitlab
        """
        url = f"{self.__class__.BASE_URL}/projects/{project_id}/merge_requests/{mr_id}/notes"
        headers = {"Content-Type": "application/json"}
        body = {"body": body}
        if (private_token is None) and (project_token is None):
            raise GitLabRequestException(
                "Missing credentials for the project token and private token"
            )
        if private_token is not None:
            headers["PRIVATE-TOKEN"] = private_token
        if project_token is not None:
            headers["PROJECT-TOKEN"] = project_token
        try:
            response = requests.post(url=url, json=body, headers=headers)
            response.raise_for_status()
        except Exception as e:
            raise GitLabRequestException(
                f"Error while connecting to the gitlab server because of error:{e}"
            )
