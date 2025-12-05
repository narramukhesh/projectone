ProjectOneflow
============
ProjectOneflow Package is metadata-driven framework which implements all data-engineering patterns as a workload with deployment in-place
​

Quick Start
------------------------
To Test Locally, run the below command 
​

Install
------------------------
Data Engineering Package is deployed on  Pypi package manager. 
To install the package:
1. Run the below code to install the code 
​
    ```shell
    pip install projectoneflow
    ```

<!-- To Get Started
------------------------

Please use below command:
```shell
projectoneflow blueprint create -o <TARGET_FOLDER_PATH>
```
Above command will be asking few questions, which generates the pipeline folder in which pipeline json template is created following your answers. You need to specify \<TARGET_FOLDER_PATH\> which is used to write the generated template files, if not specified it saves to current directory.
​ -->
​
## Let's discuss project structure
<!-- start project structure -->
The whole package is structured in same way as described as under namespace projectoneflow with sub-module in this namespace is a folder in source project folder
​
### Below are the modules:
​
**cli:** contains code related to cli command implementation, and reference to sub-commands implementation
​

**exception:** contains code related to custom exceptions which are used to raise in this project

​
**execution:** contains code related to execution operators and task context implementation

​
**observability:** contains code related to logging, instrumentation, event-listener implementation

​
**pipeline:** contains code related to different deployment like terraform etc

​
**schemas:** contains all schema definition which ever used in this package

​
**secrets:** contains implementation of task specific secret scope manager 

​
**state:** contains code related task specific state manager

**task:** contains code related to task specific implementation

**utils:** contains code related to utilities used in this package
​

All above modules are placed under `src/projectoneflow` folder

<!-- end project structure -->


ProjectOneflow Design
-----------------
1. Every pipeline/tranformation in data-engineering can be expressed as three stages which are `input -> execution -> output`
2. To explain further, input corresponds to source/producer from where we are extracting data for transformation
3. Execution stage is where core transformation logic is defined which takes input/producer data and applies some transformations and returns the transformed data
4. Ouput stage is where transformed data is written to consumer/sink.
5. By following above flow as the foundational design, on top of it each stage will be moving in different state, so to capture that projectoneflow follows the operator model
6. Where each stage is a operator which follows the flow `pre-step execution -> stage -> post-step execution`, here pre-step and post-step are configured with each operator as features.
7. These operators will operator in sequence using task model, where each task has there implementation with will have support state management, logging, event-listeners.
8. On top of these, task are executed by the pipeline. where pipeline is wrapper to execute the task as dag. Pipeline are deployed in databricks, some other enviornments using terraform provider or In future extendable provider.


To Refer more about the commands or API documentation, please refer this [docs 🔗](https://github.com/narramukhesh/projectone/tree/main/projectoneflow).
