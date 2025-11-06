package io.github.narramukhesh.projectone.spark.odata

import java.sql.Timestamp
import org.apache.spark.sql.connector.read.streaming.Offset
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession
import java.io._
import java.nio.charset.StandardCharsets
import io.github.narramukhesh.projectone.spark.odata.odatahelper.DefaultODataVariables

/** This is data-structure to hold the stream offset for micro-stream batch
  * execution
  *
  * @param value
  */
case class ODataStreamOffset(
    value: Timestamp,
    startValue: Timestamp,
    totalPage: Int,
    currentPage: Int,
    rowsPerPage: Int,
    recordsCount:Int,
    pagesLimit:Int = 0,
    limit: Int = 0,
) extends Offset {

  private val VERSION = 3
  private val template =
    "{\"version\":%d,\"field\":\"%s\",\"offset\":%d,\"total_pages\":%d,\"current_page\":%d,\"rows_per_page\":%d,\"start_value\":%d,\"records_count\":%d,\"pages_limit\":%d,\"limit\":%d}"

  override def json() = {
    val jsonString = template.format(
      VERSION,
      DefaultODataVariables.INCREMENTAL_FIELD,
      value.toInstant.toEpochMilli,
      totalPage,
      currentPage,
      rowsPerPage,
      startValue.toInstant.toEpochMilli,
      recordsCount,
      pagesLimit,
      limit
    )
    println(s"INFO: offset converted to string ${jsonString}")
    jsonString
  }
}

object ODataStreamOffset {
  // val defaultFieldOffset = "2000-01-01 00:00:00"
  // val initialOffset = new ODataStreamOffset(
  //   value = Timestamp.valueOf(defaultFieldOffset),
  //   startValue = Timestamp.valueOf(defaultFieldOffset),
  //   totalPage = 0,
  //   currentPage = 0,
  //   rowsPerPage = 0,
  //   limit = 0
  // )

  def getInitialOffsetFromValue(value: Option[Long]): ODataStreamOffset = {
    value match {
      case Some(v) =>
        new ODataStreamOffset(
          value = new Timestamp(v),
          startValue = new Timestamp(v),
          totalPage = 0,
          currentPage = 0,
          rowsPerPage = 0,
          recordsCount=0,
          pagesLimit = 0,
          limit = 0
        )
      case None =>
        new ODataStreamOffset(
          value = new Timestamp(DefaultODataVariables.EARLIEST_TIME),
          startValue = new Timestamp(DefaultODataVariables.EARLIEST_TIME),
          totalPage = 0,
          currentPage = 0,
          rowsPerPage = 0,
          recordsCount=0,
          pagesLimit =0,
          limit = 0
        )
    }
  }

  def fromJson(value: String): ODataStreamOffset = {

    var offset = ujson.read("{}")
    try {
      offset = ujson.read(value)
      println(s"INFO: serialized value $offset")
    } catch {
      case _: Throwable =>
        new Exception(
          "Problem with the parsing the json string version of offset"
        )
    }

    if (
      (offset.obj.getOrElse("version", None) == None) || (offset.obj.getOrElse(
        "offset",
        None
      ) == None) || (offset.obj.getOrElse(
        "start_value",
        None
      ) == None) || (offset.obj.getOrElse(
        "total_pages",
        None
      ) == None) || (offset.obj.getOrElse(
        "current_page",
        None
      ) == None) || (offset.obj.getOrElse("rows_per_page", None) == None)
    ) {
      throw new Exception(
        "Missing keys in offset while reading for getting the provided offset"
      )
    }

    val offsetExtr = offset("offset").num.toLong
    val startValue = offset("start_value").num.toLong
    val totalPage: Int = offset("total_pages").num.toInt
    val currentPage: Int = offset("current_page").num.toInt
    val rowsPerPage: Int = offset("rows_per_page").num.toInt
    val limit: Int = offset("limit").num.toInt
    val recordsCount:Int = if (offset("version").num.toInt<=2) -1 else offset("records_count").num.toInt
    val pagesLimit:Int = if (offset("version").num.toInt<=2) 0 else offset("pages_limit").num.toInt
    println(s"INFO: offset from provided json string ${offsetExtr}")
    ODataStreamOffset(
      value = new Timestamp(offsetExtr),
      startValue = new Timestamp(startValue),
      totalPage = totalPage,
      currentPage = currentPage,
      rowsPerPage = rowsPerPage,
      recordsCount = recordsCount,
      pagesLimit = pagesLimit,
      limit = limit
    )

  }
}

/** This is class used for writing the initial checkpoint source file when not
  * available
  *
  * @param sparkSession
  * @param metaDataPath
  */
class ODataInitialOffsetWriter(
    sparkSession: SparkSession,
    metaDataPath: String
) extends HDFSMetadataLog[ODataStreamOffset](sparkSession, metaDataPath) {

  /** This class writes the offset to checkpoint location
    * @param metadata:
    *   ODataStreamOffset object
    * @param out:
    *   Output stream object for checkpoint source directory
    */

  override def serialize(
      metadata: ODataStreamOffset,
      out: OutputStream
  ): Unit = {
    println("INFO: serializing the offset writing to checkpoint location")
    val writer = new BufferedWriter(
      new OutputStreamWriter(out, StandardCharsets.UTF_8)
    )
    writer.write(metadata.json())
    writer.flush
  }

  /** This class gets the offset json into ODataStreamOffset serialized format
    *
    * @param in:
    *   Input Stream for checkpoint directory
    * @return
    *   a ODataStreamOffset object
    */
  override def deserialize(in: InputStream): ODataStreamOffset = {
    println("INFO: deserializing the offset reading from checkpoint location")
    val content =
      IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
    require(content.nonEmpty)
    ODataStreamOffset.fromJson(content)
  }
}
