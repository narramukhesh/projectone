OneFlow-Framework
============
OneFlow-Framework Package is wrapper over the oneflow package which provides all data ingestion patterns
​

Quick Start
------------------------
To Test Locally, Please Follow below steps
​

Install
------------------------
Data Engineering Package is deployed on Pypi package manager. 
To install the package:
1. Run the below code to install the code 
​
    ```shell
    pip install oneflow-framework
    ```
Basic Idea
------------------------
Before getting to how to get started, let's understand the what are all concepts are involved in this project.

This Framework introduces the concept contract, where any data project can be defined as contract, where contract has some assets/objects where data stakeholders are involved create those assets/object.

We need the contract specification to define the assets, Currently framework support one contract type called project contract.

Project contract specification defines datasets, pipelines, transform, deploy as its assets.

Lets talk about each asset in details:

**Datasets:** This assets hold the data objects like database schema, tables, views

**Pipelines:** This assets hold the data pipeline objects, pipeline tasks, pipeline transformation logic

**Transform:** This asset hold the module/re-usable logic used across the pipelines

**Deploy:** This asset will have configuration where above defined assets are deployed.

below sections has some instruction to get started


To Get Started
------------------------
This package comes with command `oframework`. for quick reference for all sub-commands. Please use the command `oframework -h`.

1. To get started with framework, we can get the project contract set up using the below command
    Please use below command:
    ```shell
    oframework blueprint generate contract -f <TARGET_FOLDER_PATH> -c <CONTRACT_NAME>
    ```
    Above command will automatically generates the contract specification at provided folder.

    Once you have ran the above command, it creates the below folder structure at specified <TARGET_FOLDER_PATH>.
    ```
    <CONTRACT_NAME>/
        pipeline/
        dataset/
        transform/
        <CONTRACT_NAME>.json
    ```
    As defined above folder structure, command creates the the folder <CONTRACT_NAME> under <TARGET_FOLDER_PATH>. where pipeline, dataset, transform folders are specific to project contract assets. But by default project contract specification is created but command has option to specify the contract type under option `--contract_type <CONTRACT_TYPE>`.
    Created <CONTRACT_NAME>.json has the specification as below template.
    ```
    {
        "name":"<CONTRACT_NAME>",
        "description":"",
        "dataset":["datasets"],
        "pipelines":["pipelines"],
        "transform":["transform"],
        "deploy":{
            "databricks":null
        }
    }
    ```

2. To get started with the pipeline specification, we can use the same blueprint command with different sub-command as follows
    ```shell
    oframework blueprint generate pipeline -f <TARGET_FOLDER_PATH>
    ```
    Above command generates the pipeline specification at the pipeline folder in which pipeline json template is created following your answers. You need to specify \<TARGET_FOLDER_PATH\> which is used to write the generated template files, if not specified it saves to current directory.

3. To get started with dataset specification, we can steps as above but for dataset we need to specify the data object type
    ```shell
    oframework blueprint generate dataset -n <DATAOBJECT_NAME> -t {schema,table,view} -f <TARGET_FOLDER_PATH>
    ```
    Above command generates the dataset of data object specification at the specified \<TARGET_FOLDER_PATH\> folder. Based on dataobject type specified it creates the template file.

Advance Commands
------------------------------
1. To deploy the project contract, you can use the below command but requires the target deployment environment credentials either specified in project contract deploy specification or global environment variables. Please refer [environment section](#environment-variables)
    ```shell
    oframework deploy -f <PROJECT_FOLDER> --environment {local,dev,test,uat,prod}
    ```
    Deployment and destroying uses the terraform as this default deployment provider, but in future other provider can be specified based on availability.
2. For destroying the project contract, you can use same command as above. Below is the example
    ```shell
    oframework destroy -f <PROJECT_FOLDER> --environment {local,dev,test,uat,prod}
    ```
    Destroying resource same as deployment but terraform state file need to same as deployment else some error or redeployment/destroying will happens. where state management can be done same as terraform state management and pass backend_config same as with terraform commands.
3. For quick testing the pipeline, we can use the below command
    ```shell
    oframework run -f <PROJECT_FOLDER> --environment {local,dev,test,uat,prod} -p <PIPELINE_NAME> -t <CLUSTER_ID>
    ```
    Based on environment, pipeline deployment and running differs. In local environment it run pipeline in separate process and log it in temporary log file. where as other environment, pipeline will be deployed in target environment and run the pipeline and destroys post running the pipeline.

    If you are running in other than local environment, which runs on databricks <CLUSTER_ID> can be helpful to run the pipeline faster without creating the new cluster


Environment Variables
------------------------

As this framework, primary interface is command line interface. Where execution of project specification requires some configuration to be set based on environment and purpose like deployment or running or some security reasons. So some global environment variables are defined, so that developer/projects can use these global environment variables.

Below are the list of the global environment variables:

**OF_TF_DATABRICKS_CLIENT_ID:** Terraform deployment databricks client id for autentication

**OF_TF_DATABRICKS_CLIENT_SECRET:** Terraform deployment databricks client secret for autentication

**OF_TF_DATABRICKS_ACCESS_TOKEN:** Terraform deployment databricks access token for autentication

**OF_TF_DATABRICKS_WORKSPACE:** Terraform deployment databricks workspace url where resource are deployed

**OF_TF_DATABRICKS_DEPLOY_CLUSTER_ID:** Databricks cluster where running the pipeline cluster replace the provided cluster id

**OF_TF_BACKEND_CONFIG:** Terraform backend configuration specified in json configuration

**OF_TF_DATABRICKS_CATALOG:** Databricks catalog where all dataset objects are deployed

**OF_DATABRICKS_ARTIFACTS_PATH:** Databricks artifacts path where project pacakge 
stored

**OF_PRESETTING_NAME_PREFIX:** Contract pre-setting name prefix, if specified all resources are prefixed by this setting

**OF_PRESETTING_NAME_SUFFIX:** Contract pre-setting name prefix, if specified all resources are prefixed by this setting

**OF_PIPELINE_TAGS:** Contract pre-setting pipeline tags, if specified tags added to pipeline resources

**OF_PIPELINE_TASK_CHECKPOINT_LOCATION_PREFIX:** For stream pipeline processing require checkpoint location, this setting provide prefix to the checkpoint location of all tasks

**OF_PIPELINE_METADATA_LOCATION:** Metadata location where pipeline state are stored

**OF_PRESETTING_TAGS:** Contract pre-setting tags in json form, if specified all resources are specified with these tags by this setting

**OF_DATABRICKS_SECRET_SCOPE:** Secret scope environment variable used to specify for the environment specific databricks secret scope

**OF_DEPLOY_CORE_PACKAGE_PATH:** This is used to specify the library/path where oneflow-framework uses to run the pipeline, by default oneflow is taken but if you want untested version of oneflow, you can specify it here.

**OF_DEPLOY_CORE_PACKAGE_TYPE:** Type to be specified by default this should be specified with `whl`, if you specify the `OF_DEPLOY_CORE_PACKAGE_PATH` and this variable also need to defined