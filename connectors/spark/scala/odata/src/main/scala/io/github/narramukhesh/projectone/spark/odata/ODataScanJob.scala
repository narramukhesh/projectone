package io.github.narramukhesh.projectone.spark.odata

import io.github.narramukhesh.projectone.spark.odata.odatahelper._
import io.github.narramukhesh.projectone.spark.odata.odataEngine.{ODataConnectionSpec}

/** This is configuration holder for each table scan job
  *
  * @param odataSpec:
  *   OData connection specification object
  * @param schema:
  *   odata schema object
  * @param rowsPerPage:
  *   No. of rows per fetch
  * @param limit:
  *   Limit data
  * @param pushedFilter:
  *   filter to applied at odata endpoint
  * @param tableName:
  *   OData Endpoint name
  * @param selectedColumn:
  *   columns to be selected from odata entity
  * @param maxPagesToProcess:
  *   maximum pages to be processed by the micro-batch
  * @param earliestTime:
  *  earliestTime to be specified for the micro-batch starting time
  */
case class ODataScanJob(
    odataSpec: ODataConnectionSpec,
    schema: ODataSchema,
    rowsPerPage: Option[Int] = Some(DefaultODataVariables.ROWS_PER_FETCH),
    limit: Option[Int] = None,
    pushedFilter: Option[Array[PushedFilter]] = None,
    tableName: Option[String] = None,
    selectedColumn: Option[Array[String]] = None,
    maxPagesToProcess: Option[Int] = Some(DefaultODataVariables.MAX_PAGES_PER_BATCH),
    earliestTime:Option[Long] = Some(DefaultODataVariables.EARLIEST_TIME)
) {}
