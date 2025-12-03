from oneflow.core.schemas import ParentModel, ParentEnum
from typing import Protocol, runtime_checkable, List, Dict, Any, Optional
from pydantic import Field


class DeployStrategyTypes(ParentEnum):
    """This is the enum definition for the deploy strategy types"""

    terraform = "terraform"


class DeployStateTypes(ParentEnum):
    local = "local"
    remote = "remote"


class DeployStateResult(ParentModel):
    type: DeployStateTypes = Field(
        DeployStateTypes.local, description="deploy state where state is stored"
    )
    local_location: str = Field(..., description="deploy state where state is stored")
    lock_id: Optional[str] = Field(
        None,
        description="lock id if target remote state is using the mechanism for consistency",
    )
    remote_state_location: str = Field(
        ..., description="deploy state location where remote/local state will be stored"
    )
    remote_state_config: Optional[Dict[str, Any]] = Field(
        None,
        description="remote state configuration used for the connecting with remote",
    )


class DeployInitOutput(ParentModel):
    """This is the terraform initialization output used to represent the terraform deployment initialization output"""

    init_out: Optional[str] = Field(None, description="terraform initialization output")
    state_out: Optional[DeployStateResult] = Field(
        None, description="terraform state result output"
    )


class DeployBuildOutput(ParentModel):
    """This is the build output used to represent the deployment output"""

    deploy_dir: str = Field(
        ..., description="Deployment directory used to deploy the target artifacts"
    )
    targets: List[str] = Field(..., description="target resources to be deployed ")


class DeployPlanOutput(ParentModel):
    """This is the build output used to represent the deployment output"""

    plan_file: str = Field(
        ..., description="plan state file used for the state deployment"
    )
    plan_json: Dict[str, Any] = Field(..., description="plan state file in json format")


class DeployApplyOutput(ParentModel):
    """This is the build output used to represent the deployment output"""

    deploy_output: Dict[str, Any] = Field(
        ..., description="output configuration returned after applying the deployment"
    )


@runtime_checkable
class DeployStrategy(Protocol):
    """This class is a definition for deploy strategy"""

    def deploy(self):
        """This is the method where child classes will be implemented"""

    def destroy(self):
        """This method destroy the target resources which are implemented"""

    def deploy_build(self):
        """This method helps to build the target resources which are used to implement"""

    def deploy_initialize(self):
        """This method helps to initialize the built target resources"""

    def deploy_plan(self):
        """This method helps to plan the resource to be implemented"""

    def deploy_apply(self):
        """This method helps to apply the planned changes to the target resource"""
