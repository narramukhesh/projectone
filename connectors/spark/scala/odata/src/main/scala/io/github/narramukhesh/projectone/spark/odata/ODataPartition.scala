package io.github.narramukhesh.projectone.spark.odata

import org.apache.spark.sql.catalyst.InternalRow
import io.github.narramukhesh.projectone.spark.odata.odatahelper._
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import io.github.narramukhesh.projectone.spark.odata.exception.ODataDataException
import io.github.narramukhesh.projectone.spark.odata.odataEngine.{
  ODataClient,
  ODataConnectionSpec
}

/** This is data structure hold the Input Partition for odata parition
  * configuration
  *
  * @param partitionNum
  * @param elementsToFetch
  */
case class ODataInputPartition(partitionNum: Int, elementsToFetch: Int)
    extends InputPartition {}

/** This class is the actual implementation for each paritition reader where get
  * method is a iterator for Internal Row
  *
  * @param scanJob
  * @param odataPartition
  */
class ODataParitionReader(
    scanJob: ODataScanJob,
    odataPartition: ODataInputPartition
) extends PartitionReader[InternalRow]
    with ODataHelper {

  /** This variable holds the data rowsfrom source
    *
    * @return
    */
  lazy val dataRows = {
    println(s"INFO: Extracted the data for partition ${odataPartition} ")
    val client = new ODataClient(scanJob.odataSpec)
    val page_arguments = client.constructPageExtraArguments(
      scanJob.selectedColumn,
      scanJob.pushedFilter
    )
    val result = client.getPageTableData(
      scanJob.tableName,
      page_arguments,
      odataPartition.partitionNum,
      Some(odataPartition.elementsToFetch)
    )
    result
      .map(x =>
        scanJob.schema.columns
          .map(col =>
            col.convertValueToNative(x(col.name) match {
              case string: ujson.Str   => string.str
              case number: ujson.Num   => number.num
              case boolean: ujson.Bool => boolean.bool
              case _ =>
                throw new ODataDataException(
                  "Invalid Data type/Source from odata which doesn't matches with defined structure"
                )
            })
          )
          .toSeq
      )
      .toArray
  }
  private var curIndex: Int = 0

  /** This is is next for volcano implementation
    *
    * @return
    */
  override def next(): Boolean = {
    curIndex < dataRows.length
  }

  /** This is is get yielding the row for volcano implementation
    *
    * @return
    *   InternalRow
    */

  override def get(): InternalRow = {
    val row = dataRows(curIndex)
    curIndex += 1
    InternalRow.apply(row: _*)
  }

  /** This tearup method for any closing connection, but in this we don't have
    * any
    */

  override def close(): Unit = {}
}
