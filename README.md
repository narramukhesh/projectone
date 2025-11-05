Project One
===================================

This Project will be used to develop the concepts related to data storage, processing concepts with development of internal tools related to external connectors, infra, data engineering framework, data governance, data-agentic, data bases optimizer framework etc. Where this is monorepo with support of multiple projects, where each project supports its own deployment cycle.

To start Contributing, Please check the [Contributing.md](./CONTRIBUTING.md) 

## Project Structure

**Connectors** : This folder contains all external connectors specific implementation, please check further details on connector specific documentation linked with connectors folder README.md files

**OneFlow** : This folder contains the data engineering design implementation with all data ingestion patterns, utilities. To learn more, Please follow the [link](./oneflow/README.md)

**OneFlow-Framework** : This folder contains the data engineering framework implementation with different platform deployment, validation, running the pipelines. To learn more, Please follow the [link](./oneflow-framework/README.md) 

**.precommit-config.yaml**: This file has pre commit hooks configuration

**CONTRIBUTING.md**: This file has details on contributing guidlines followed for this project

**.github**: This folder has some pre-defined templates like merge-request, issue templates useful for enforcing the rules.