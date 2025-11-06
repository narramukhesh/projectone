package io.github.narramukhesh.projectone.spark.odata

import org.apache.spark.sql.connector.catalog.{TableProvider, Table}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources._
import org.apache.spark.internal.Logging
import io.github.narramukhesh.projectone.spark.odata.exception.{RequiredOptionsNotProvidedException}
import io.github.narramukhesh.projectone.spark.odata.odataEngine.ODataConnectionSpec
import io.github.narramukhesh.projectone.spark.odata.odatahelper.DefaultODataVariables

/** This class is the implementation of OData-Spark connector where this class
  * is registered as service where spark identifies.
  */
class ODataSource
    extends TableProvider
    with DataSourceRegister
    with Logging {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    println(
      "INFO: Inside the ODataSource InferSchema which return Nothing"
    )
    log.info(
      "INFO: Inside the ODataSource InferSchema which return Nothing"
    )
    null
  }
  override def shortName(): String = {
    "odata"
  }

  /** This method is the one called when spark load operation is performed
    *
    * @param schema
    * @param partitioning
    * @param properties
    * @return
    */
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {

    log.info(
      s"INFO: properties/options provided with ODataSource are $properties"
    )
    println(
      s"INFO: properties/options provided with ODataSource are $properties"
    )

    val tableName: Option[String] =
      if (properties.containsKey("path")) Some(properties.get("path")) else None
    if (
      (!properties.containsKey("clientID") && !properties.containsKey("clientid"))||
      (!properties.containsKey("clientSecret") && !properties.containsKey("clientsecret")) ||
      (!properties.containsKey("identityUrl") && !properties.containsKey("identityurl")) ||
      (!properties.containsKey("instanceUrl") && !properties.containsKey("instanceurl"))
    ) {
      throw new RequiredOptionsNotProvidedException(
        "clientId, clientSecret, identityUrl, serviceUrl must be provided"
      )
    }
    val rowsPerPage: Option[Int] =
      if (properties.containsKey("row_per_page_fetch"))
        Some(properties.get("row_per_page_fetch").toInt)
      else Some(DefaultODataVariables.ROWS_PER_FETCH)
    val maxPagesToProcess: Option[Int] =
      if (properties.containsKey("max_pages_to_process"))
        Some(properties.get("max_pages_to_process").toInt)
      else Some(DefaultODataVariables.MAX_PAGES_PER_BATCH)
    val readConnectionTimeout: Option[Int] =
      if (properties.containsKey("read_connection_timeout"))
        Some(properties.get("read_connection_timeout").toInt)
      else Some(DefaultODataVariables.READ_CONNECTION_TIMEOUT)
    val connectionTimeout: Option[Int] =
      if (properties.containsKey("connection_timeout"))
        Some(properties.get("connection_timeout").toInt)
      else Some(DefaultODataVariables.CONNECTION_TIMEOUT)

    val fieldsToBeSelected: Option[Array[String]] =
      if (properties.containsKey("fields_to_be_selected"))
        Some(properties.get("fields_to_be_selected").split(",").filter(_.nonEmpty))
      else None
    
    val predicates: Option[Array[String]] =
      if (properties.containsKey("predicates"))
        Some(properties.get("predicates").split(",").filter(_.nonEmpty))
      else None
    val earliestTime: Option[Long] = if (properties.containsKey("earliest_time"))
        Some(properties.get("earliest_time").toLong)
      else Some(DefaultODataVariables.EARLIEST_TIME)
    
    val clientId: String = if (properties.containsKey("clientID"))
        "clientID"
      else "clientid"
    
    val clientSecret: String = if (properties.containsKey("clientSecret"))
        "clientSecret"
      else "clientsecret"
    
    val instanceUrl: String = if (properties.containsKey("instanceUrl"))
        "instanceUrl"
      else "instanceurl"
    
    val identityUrl: String = if (properties.containsKey("identityUrl"))
        "identityUrl"
      else "identityurl"
    
    ODataTable(
      odataSpec = new ODataConnectionSpec(
        properties.get(clientId),
        properties.get(clientSecret),
        properties.get(identityUrl),
        properties.get(instanceUrl),
        readConnectionTimeout,
        connectionTimeout
      ),
      tableName = tableName,
      rowsPerPage = rowsPerPage,
      maxPagesToProcess = maxPagesToProcess,
      fieldsToBeSelected = fieldsToBeSelected,
      earliestTime=earliestTime,
      predicates=predicates
    )
  }
}
