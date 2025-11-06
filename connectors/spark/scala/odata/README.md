OData Spark Connector
================================

## Okay, Understood then why do we need Spark connector 🤔?

As Spark is popular data processing framework and PAAS service build upon it like the databricks . Currently `creatio` like crm source using the odata http protocol to expose data which is missing the spark native connection. So to accomdate future data analysis and for cleaner design approach new spark connector will be helpful.

## Let's now Understand the code flow for odata Spark Connector

1. Spark provides the plug-in framework for creating the custom datasources.
2. To register the custom spark datasources, we need to implement a `TableProvider` class which extends the `DataSourceRegister` class. In creatio case, the implementation is `ODataDataSource` which is registered as java service under `resources/META-INF/services` folder
3. Now spark recognises the newly created data source, to create a data frame it will call `getTable` method which should return the `Table` with has schema and scanbuilder. In this implementation the corresponding class is `ODataTable`
4. When `ODataTable` is send to spark dataframe loader, it check the schema attribute of `ODataTable` for dataframe schema. In our case it is inferred from API (Ex: https://example.com/0/odata/$metadata)
5. When any action performed on the spark dataframe, then it calls the scanBuilder which in our `ODataScanBuilder` which helps in building the logicalPlan where all push down optimization is applied. This step invokes the `ODataScan`.
6. In `ODataScan` step it plans the no. of partitions, current implementation get the count of rows by applying all push-down filters. After getting the count, it decides the partition based on (count of rows)/(rows per page) where rows per page is a configuration passed while configuring the pagination of rest-api.
7. Where after planning we get the partitions, each partition is instanted with reader in executor, In our implementation `ODataParitionReader` is the reader.

**__NOTE__:**  Above explaination is for spark batch dataframe reading but most parts holds true for micro-batch

## Let's see examples with explaination

### Below is the example of reading the data from odata data source. for example we cna use the creatio crm http application
```python
import pyspark.sql.SparkSession

spark = SparkSession.builder().master("local[2]").getOrCreate()

data = spark.read.format("odata").options(**{"clientID" : "xxxx","clientSecret":"xxxx","identityUrl" :"https://xxxx-is.creatio.com","instanceUrl":"https://xxxx.creatio.com"}).load("Account")

```

Let's see options available in this data source:
1. clientID [**Required**][**String**]: This is clientID/username to autenticate with creatio identity service
2. clientSecret [**Required**][**String**]: This is clientSecret/password to autenticate with creatio identity service
3. instanceUrl [**Required**][**String**]: This is rest-api endpoint to fetch the data
4. identityUrl [**Required**][**String**]: This is the identity url for autentication and for fetching the access token
5. rowsPerPage [**Optional**][**Integer**]: This option decide the no.of rows per page which is crucial in no.of partitions, so high config value -> less spark partition, low config value -> more spark partition and also no.of partition depends on actual data count.
6. max_pages_to_process [**Optional**][**Integer**]: This Configuration is used for the micro-stream execution to limit the no. of pages to be executed in single batch when enabled using the available now trigger
7. fields_to_be_selected [**Optional**][**String**]: This Configuration is used to limit the no. of columns to be pruned as catalyst optiizer is not pushing the columns for micro-batch load
8. read_connection_timeout [**Optional**][**Integer**]: Time in milli seconds which is specified for request to timeout
9. connection_timeout [**Optional**][**Integer**]: Time in milli seconds to establish the request connection timeout

`path` which is specified in load() is used as entity name to extract the data

### Below is the example of micro-batch spark streaming

```python
import pyspark.sql.SparkSession

spark = SparkSession.builder().master("local[2]").getOrCreate()

data = spark.readStream.format("odata").options(**{"clientID" : "xxxx","clientSecret":"xxxx","identityUrl" :"https://xxxx-is.creatio.com","instanceUrl":"https://xxxx.creatio.com"}).load("Account")

```

As you see all options defined for batch applicable for micro-batch. 

Micro-batch stream depends on `INCREMENTAL_FIELD` variable for incremental processing in the context of the creatio source that field is `ModifiedOn` field in creatio rest-api. In this mode, depends on trigger time specified it queries the rest-api with filter on `INCREMENTAL_FIELD` field between last offset value to current time value. Even no data available, this stream runs which may result in empty files, so be cautious with entity name choosing for the micro-stream. This issue can be solved by checking the `getEntityCount` method to see any data available, if no data `latestOffset` should be previous offset processed.

## Not Supported

1. `Continuos Stream` mode is not supported
2. No write support

## Example Creatio Source References
1. For Creatio API Documentation check this [🔗](https://documenter.getpostman.com/view/10204500/SztHX5Qb?version=latest#8f35ebd2-e386-4b67-986d-d9eb3c13c668)
2. For Creatio Academy Documentation check this [🔗](https://academy.creatio.com/docs/developer/integrations_and_api)



