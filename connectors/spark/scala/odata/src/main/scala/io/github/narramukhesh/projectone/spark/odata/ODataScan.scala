package io.github.narramukhesh.projectone.spark.odata

import io.github.narramukhesh.projectone.spark.odata.ODataScanJob
import io.github.narramukhesh.projectone.spark.odata.odataEngine.{
  ODataClient,
  ODataConnectionSpec
}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.{
  SupportsPushDownFilters,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.connector.read.SupportsPushDownLimit
import org.apache.spark.sql.sources._
import io.github.narramukhesh.projectone.spark.odata.odatahelper._
import org.apache.spark.internal.Logging

/** This class is executed for each logic planning phase where all optimization
  * like column pruning, filter pruning will be happens here
  *
  * @param scanJob
  */
class ODataScanBuilder(scanJob: ODataScanJob)
    extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownLimit
    with ODataHelper {

  private var _schema: ODataSchema = scanJob.schema
  private var _limit: Option[Int] = None
  private var _pushedFilters =
    scala.collection.mutable.ArrayBuffer.empty[PushedFilter]
  private var _schemaFields: Option[Array[String]] = None

  /** Builds the scan object for specific workload
    *
    * @return
    */
  override def build(): Scan = {
    new ODataScan(
      scanJob.copy(
        schema = this._schema,
        limit = this._limit,
        pushedFilter = if (scanJob.pushedFilter.isDefined) Some(((scanJob.pushedFilter.get.toSeq) ++ (_pushedFilters.toSeq)).toArray) else Some(_pushedFilters.toArray),
        selectedColumn =  if (this._schemaFields.isDefined) this._schemaFields else scanJob.selectedColumn
      )
    )
  }

  /** This methods returns the filters that can't be pushed to source
    */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {

    val push_filters = parseFilters(filters)

    val not_applied_filter = filters.filter(filter =>
      !push_filters.map(_.rawFilter).toArray.contains(filter)
    )

    _pushedFilters ++= push_filters

    println(push_filters)
    not_applied_filter
  }

  /** This methods returns the filters that are pushed to source
    */
  override def pushedFilters(): Array[Filter] = {
    _pushedFilters.map(_.rawFilter).toArray
  }

  /** This is a helper method which parses the filter into PushedFilter and
    * determines which filters are applicable to our implementation
    */

  def parseFilter(
      attribute: String,
      value: Any,
      rawFilter: Filter,
      operator: String
  ): Option[PushedFilter] = {
    val column_with_schema = this._schema.columns.find(_.name == attribute)

    column_with_schema match {
      case Some(field) => {
        if (checkType(field.targetType)) {
          Some(
            PushedFilter(
              field.sourceName,
              operator,
              parseValue(value, field.sourceType),
              rawFilter
            )
          )
        } else {
          None
        }
      }
      case _ => None
    }

  }

  /** This is a helper method which parses the filters into Array(PushedFilter)
    * and determines which filters are applicable to our implementation
    */

  def parseFilters(filters: Array[Filter]): Iterable[PushedFilter] = {

    filters
      .map {
        case LessThan(attr, value) =>
          parseFilter(attr, value, LessThan(attr, value), "<")
        case GreaterThan(attr, value) =>
          parseFilter(attr, value, GreaterThan(attr, value), ">")
        case GreaterThanOrEqual(attr, value) =>
          parseFilter(attr, value, GreaterThanOrEqual(attr, value), ">=")
        case EqualTo(attr, value) =>
          parseFilter(attr, value, EqualTo(attr, value), "=")
        case LessThanOrEqual(attr, value) =>
          parseFilter(attr, value, LessThanOrEqual(attr, value), "<=")
        case StringStartsWith(attr, value) =>
          parseFilter(attr, value, StringStartsWith(attr, value), "startsWith")
        case StringEndsWith(attr, value) =>
          parseFilter(attr, value, StringEndsWith(attr, value), "endsWith")
        case StringContains(attr, value) =>
          parseFilter(attr, value, StringEndsWith(attr, value), "contains")
        case _ => None
      }
      .filter(_.nonEmpty)
      .map(_.get)

  }

  /** This method prunes the infered schema, helpful for the column projection
    * push down
    */

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredSchemacols = requiredSchema.map(_.name)
    this._schema = ODataSchema(
      _schema.columns.filter(field => requiredSchemacols.contains(field.name))
    )
    this._schemaFields =
      if (requiredSchemacols.length <= this._schema.columns.toArray.length)
        Some(this._schema.columns.map(_.name).toArray)
      else None

  }

  /** This method push the limit on data retreival from source
    */
  override def pushLimit(limit: Int): Boolean = {
    this._limit = Some(limit)
    true
  }

}

/** This Scan is called for each time any action is applied on the source where
  * logical plan to physical plan Currently this scan implementation supports
  * batch and micro-batch stream read mode
  */
class ODataScan(scanJob: ODataScanJob)
    extends Scan
    with Batch
    with PartitionReaderFactory
    with Logging {

  /** This is the final schema after pruning which is used for physical plan
    *
    * @return
    */
  override def readSchema(): StructType = scanJob.schema.sparkSchema

  /** This method point to actual execution of batch mode
    *
    * @return
    */
  override def toBatch(): Batch = {
    this
  }

  /** This method point to actual micro-batch stream execution
    */

  override def toMicroBatchStream(checkpointLocation: String) = {
    log.info(s"INFO: Microbatch Stream scan odatan")
    println(s"INFO: Microbatch Stream scan odatan")
    new ODataMicroBatchStream(scanJob, checkpointLocation)
  }

  /** This method is build the partition reader in batch mode
    *
    * @return
    */
  override def createReaderFactory(): PartitionReaderFactory = {
    log.info(
      s"INFO: Partition factory to create the partition reader for batch"
    )
    println(s"INFO: Partition factory to create the partition reader for batch")
    this
  }

  /** This method is plans the partitions based on entity count after applying
    * the filters, limit
    */
  override def planInputPartitions(): Array[InputPartition] = {

    val odataClient: ODataClient = new ODataClient(scanJob.odataSpec)
    val noOfRows: Int = scanJob.limit match {
      case Some(limit) => limit
      case None =>
        odataClient.getEntityCount(scanJob.tableName, scanJob.pushedFilter)

    }
    val noOfPartitions: Int = {
      scanJob.rowsPerPage match {
        case Some(rows) => {
          if (scanJob.tableName.isDefined) (noOfRows / rows) else 0
        }
        case None => 0
      }
    }

    val topLimt: Int = {
      scanJob.rowsPerPage match {
        case Some(rows) => Math.min(noOfRows, rows)
        case None       => noOfRows
      }
    }

    val rangeOfPartitions: Range = (0 to noOfPartitions)

    log.info(
      s"INFO: Creating $noOfPartitions OData Input Partitions for batch "
    )
    println(
      s"INFO: Creating $noOfPartitions OData Input Partitions for batch "
    )

    rangeOfPartitions
      .map(paritionNum => {
        new ODataInputPartition(paritionNum, topLimt)
          .asInstanceOf[InputPartition]
      })
      .toArray

  }

  /** This method is executed which returns the reader to be executed by
    * executor to fetch the data
    */

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {

    val odataParition: ODataInputPartition =
      partition.asInstanceOf[ODataInputPartition]

    log.info(
      s"INFO: Partition Reader from odata Datasource for ${partition} "
    )
    println(s"INFO: Partition Reader from odata Datasource for ${partition} ")

    new ODataParitionReader(scanJob.copy(), odataParition)
  }

}
