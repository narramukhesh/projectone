from oneflow.core.schemas.deploy import (
    SparkPipelineConfig,
    SparkTask,
    SparkPipelineTask,
    SparkPipelineDeloyConfig,
    DatabricksDeployConfig,
)
from cdktf import Testing, TerraformStack
from oneflow.core.deploy.terraform import TerraformComponent
from cdktf_cdktf_provider_databricks.job import Job


def test_spark_pipeline_config_schema(get_example_spark_pipeline_config):
    """This test checks whether the spark pipeline config is correctly convert to corresponding schema object"""

    spark_pipeline_config = get_example_spark_pipeline_config[1]

    spark_pipeline_object = SparkPipelineConfig(**spark_pipeline_config)
    assert isinstance(spark_pipeline_object.tasks["test_spark_task"], SparkTask)
    assert isinstance(
        spark_pipeline_object.tasks["test_spark_pipeline_task"], SparkPipelineTask
    )


def test_databricks_pipeline_terraform_deployment(get_example_spark_pipeline_config):
    """This test checks whether the spark pipeline terraform success"""

    spark_pipeline_config = [
        SparkPipelineDeloyConfig(**pipeline)
        for pipeline in get_example_spark_pipeline_config
    ]

    databricks_deploy_config = DatabricksDeployConfig(pipeline=spark_pipeline_config)
    testing_app = Testing.app()
    terraform_stack = TerraformComponent(
        testing_app, "test_databricks_pipeline_terraform_deployment"
    )
    terraform_stack.add_components("databricks", databricks_deploy_config)
    synthetised = Testing.synth(terraform_stack)
    # valid provider databricks
    assert Testing.to_have_provider(synthetised, "databricks")
    # valid resource
    assert Testing.to_have_resource(synthetised, Job.TF_RESOURCE_TYPE)
    # valid resource properties
    assert Testing.to_have_resource_with_properties(
        synthetised, Job.TF_RESOURCE_TYPE, {"name": "test_spark_pipeline_job"}
    )
    # valid resource properties
    assert Testing.to_have_resource_with_properties(
        synthetised, Job.TF_RESOURCE_TYPE, {"name": "test_spark_pipeline_config"}
    )
    # valid resource properties
    assert Testing.to_have_resource_with_properties(
        synthetised, Job.TF_RESOURCE_TYPE, {"name": "test_spark_pipeline_config"}
    )
