package io.github.narramukhesh.projectone.spark.odata

import io.github.narramukhesh.projectone.spark.odata.odatahelper._
import org.apache.spark.sql.connector.read.streaming.{
  MicroBatchStream,
  Offset,
  SupportsTriggerAvailableNow,
  ReadLimit,
  ReadMaxRows
}
import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.catalyst.InternalRow
import io.github.narramukhesh.projectone.spark.odata.odataEngine.{
  ODataClient,
  ODataConnectionSpec
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.{LessThan, GreaterThanOrEqual}
import io.github.narramukhesh.projectone.spark.odata.exception.ODataRecordsMisMatchException

/** This is class is micro-batch implementation of odata data source, where
  * spark uses it to extract the data in micro-stream batch mode based on
  * Trigger It is where all parition planning happens, currently incremental
  * ingestion happens based on `ModifiedOn` column filter Currently this
  * implementation, runs if there is no data in source. so its upto target
  * handle the no rows insert case. But this can be handled by provided the
  * noDataAvailable flag, but it will be implemented in next version
  */
class ODataMicroBatchStream(
    scanJob: ODataScanJob,
    checkpointLocation: String
) extends SupportsTriggerAvailableNow
    with MicroBatchStream
    with ODataHelper
    with PartitionReaderFactory
    with Logging {

  var _pushedFilter: Option[Array[PushedFilter]] = scanJob.pushedFilter
  private var latestODataOffset: Offset = _
  private var availableNowDataOffset: Offset = _

  private val maxPagesToProcess: Int = scanJob.maxPagesToProcess.getOrElse(4)
  private val rowsPerPage: Int = scanJob.rowsPerPage.getOrElse(1000)

  /** This method returns the initialOffset at start of each Streaming job run
    *
    * @return
    *   a ODataStreamOffset object
    */
  override def initialOffset(): Offset = {
    getInitialOffset()
  }

  /** This method returns the default read limit for admission control streaming
    *
    * @return
    */
  override def getDefaultReadLimit: ReadLimit = {
    ReadLimit.maxRows(maxPagesToProcess)
  }

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method"
    )
  }

  /** This method returns the end offset at start of each micro-batch run
    * @param start:
    *   Input Stream start offset for next micro batch
    * @param readLimit:
    *   determine the no. of max rows/pages to be read in single micro batch
    * @return
    *   a ODataStreamOffset object
    */
  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = {

    val startOffset = start.asInstanceOf[ODataStreamOffset]
    latestODataOffset = {
      if (availableNowDataOffset != null) {
        // if (
        //   startOffset.value != availableNowDataOffset
        //     .asInstanceOf[ODataStreamOffset]
        //     .value
        // ) {
        //   availableNowDataOffset = availableNowDataOffset
        //     .asInstanceOf[ODataStreamOffset]
        //     .copy(startValue = startOffset.value)
        //     .asInstanceOf[Offset]
        // }
        availableNowDataOffset
      } else {
        availableNowDataOffset = getLatestOffset(start)
        availableNowDataOffset
      }
    }
    val offset = if (readLimit == ReadLimit.allAvailable()) {
      latestODataOffset
    } else {
      rateLimit(
        start,
        latestODataOffset,
        readLimit.asInstanceOf[ReadMaxRows].maxRows().toInt
      )
    }
    offset

  }

  /** This method returns the end offset at start of each micro-batch run
    * @param start:
    *   Input Stream start offset for next micro batch
    * @param endOffset:
    *   This endoffset to which it will rate limit the batch stream
    * @param limit:
    *   This is the limit to which startoffet no.of pages the offset limit the
    *   endoffset
    * @return
    *   a ODataStreamOffset object
    */
  private def rateLimit(
      startOffset: Offset,
      endOffset: Offset,
      limit: Int
  ): Offset = {

    val start = startOffset.asInstanceOf[ODataStreamOffset]
    val latest = endOffset.asInstanceOf[ODataStreamOffset]

    if (start == latest && availableNowDataOffset != null) {
      latest.copy(pagesLimit = limit)
    } else {
      if (latest.totalPage == 0) {
        latest.copy(pagesLimit = limit)
      } else if (
        start.value == latest.value && start.startValue == latest.startValue
      ) {

        // Here is the expensive operation which checks for the pages and which checks the count of records from the api
        // This is needed because records gets shifted between the batches we need to re-adjust the current page and total page
        val updatedOffset: Offset =
          getParitionOffsetBetweenDates(start.startValue, start.value)
        val updatedODataOffset: ODataStreamOffset =
          updatedOffset.asInstanceOf[ODataStreamOffset]
        if (start.recordsCount > updatedODataOffset.recordsCount) {

          val paritionDiff: Int =
            start.totalPage - updatedODataOffset.totalPage

          if (paritionDiff > 0) {
            val currentPage =
              if ((start.currentPage - paritionDiff) < 0) -1
              else start.currentPage - paritionDiff

            availableNowDataOffset = updatedODataOffset.copy()
            latest.copy(
              pagesLimit = limit,
              currentPage =
                if ((currentPage + limit) < updatedODataOffset.totalPage)
                  limit + currentPage
                else updatedODataOffset.totalPage,
              recordsCount = updatedODataOffset.recordsCount,
              totalPage = updatedODataOffset.totalPage
            )
          } else {
            availableNowDataOffset = updatedODataOffset.copy()
            latest.copy(
              pagesLimit = limit,
              currentPage = if (
                (start.currentPage - 1 + limit) < updatedODataOffset.totalPage
              )
                limit + start.currentPage - 1
              else updatedODataOffset.totalPage,
              recordsCount = updatedODataOffset.recordsCount
            )
          }

        } else if (start.recordsCount == updatedODataOffset.recordsCount) {

          latest.copy(
            pagesLimit = limit,
            currentPage =
              if ((start.currentPage + limit) < latest.totalPage)
                limit + start.currentPage
              else latest.totalPage
          )
        } else {
          throw new ODataRecordsMisMatchException(
            s"OData Api Mismatching has more records ${DefaultODataVariables.INCREMENTAL_FIELD} between  ${start.startValue} and ${start.value} of filter when running before micro-stream batch"
          )
        }

      } else {
        latest.copy(
          pagesLimit = limit,
          currentPage =
            if (limit - 1 >= 0 && limit - 1 < latest.totalPage) limit - 1
            else latest.totalPage
        )
      }

    }

  }

  /** This method returns the offset to be displayed in structured steaming
    * metrics
    *
    * @return
    *   a ODataStreamOffset object
    */
  override def reportLatestOffset(): Offset = {
    latestODataOffset
  }

  /** This method defines the available now trigger micro batch streaming
    *
    * @return
    */
  override def prepareForTriggerAvailableNow(): Unit = {
    // availableNowDataOffset = getLatestOffset(getInitialOffset())
  }

  /** This method returns the offset which has the information related to the
    * partition bewteen the date range
    * @param start:
    *   start timestamp to apply the filter to the odata api endpoint
    * @param end:
    *   end timestamp to apply the filter to the odata api endpoint as
    *   `between clause`
    * @return
    *   a ODataStreamOffset object calculated from the given start and end
    *   timestamp
    */
  private def getParitionOffsetBetweenDates(
      startTime: Timestamp,
      endTime: Timestamp
  ): Offset = {

    val startModifiedTimestampFilter: PushedFilter = PushedFilter(
      DefaultODataVariables.INCREMENTAL_FIELD,
      ">=",
      parseValue(startTime),
      GreaterThanOrEqual(DefaultODataVariables.INCREMENTAL_FIELD, startTime)
    )
    val endModifiedTimestampFilter: PushedFilter = PushedFilter(
      DefaultODataVariables.INCREMENTAL_FIELD,
      "<",
      parseValue(endTime),
      LessThan(DefaultODataVariables.INCREMENTAL_FIELD, endTime)
    )
    val pushedMicroBatchFilter: Array[PushedFilter] =
      Array(startModifiedTimestampFilter, endModifiedTimestampFilter)
    val pushedFilter = scanJob.pushedFilter match {
      case Some(filter) =>
        Some(
          filter.filter(
            _.column != DefaultODataVariables.INCREMENTAL_FIELD
          ) ++ pushedMicroBatchFilter
        )
      case None => Some(pushedMicroBatchFilter)
    }
    val odataClient: ODataClient = new ODataClient(scanJob.odataSpec)
    val noOfRows: Int = scanJob.limit match {
      case Some(limit) => limit
      case None =>
        odataClient.getEntityCount(scanJob.tableName, pushedFilter)

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

    new ODataStreamOffset(
      value = endTime,
      startValue = startTime,
      totalPage = noOfPartitions,
      currentPage = noOfPartitions,
      rowsPerPage = rowsPerPage,
      recordsCount = noOfRows,
      limit = topLimt
    )

  }

  /** This method returns the end offset at start of each micro-batch run
    * @param startOffset:
    *   start offset to be passed to get the endoffset
    * @return
    *   a ODataStreamOffset object calculated from the given start offset
    */

  private def getLatestOffset(startOffset: Offset): Offset = {

    val start = startOffset.asInstanceOf[ODataStreamOffset]

    if (start.currentPage < start.totalPage) {
      val updatedOffset: Offset =
        getParitionOffsetBetweenDates(start.startValue, start.value)
      val updatedODataOffset: ODataStreamOffset =
        updatedOffset.asInstanceOf[ODataStreamOffset]
      // This is required for starting new micro batch run where previous total pages between the run changed so it gets the updated offsets
      // Added below condiiton to avoid the inconsistent scanning of the data
      if (start.recordsCount > updatedODataOffset.recordsCount) {
        updatedOffset
      }
      start.copy(currentPage = start.totalPage)
    } else {
      val now = Instant.now().toEpochMilli
      val lastTime = new Timestamp(now)
      getParitionOffsetBetweenDates(start.value, lastTime)
    }
  }

  /** This method returns the deserialized offset from json to StreamOffset
    */

  override def deserializeOffset(json: String) = {
    println(
      s"INFO: Deserializing the json offset $json into ODataStreamOffset"
    )
    ODataStreamOffset.fromJson(json)
  }

  /** This method plans the input partitions for running the at each micro-batch
    * stream
    * @param start:
    *   start offset from when the micro-batch run
    * @param end
    *   : end offset until when micro-batch stops
    *
    * This partition planning works like this: start and end offsets are
    * converted to push filter and appended to filter passed to odata source
    * and get the enitiy count where no.of paritions = (no.of entity
    * count)/(no.of rows per page) where no.of rows per page is user supplied
    * configuration
    */

  override def planInputPartitions(
      start: Offset,
      end: Offset
  ): Array[InputPartition] = {

    val startOffset = start.asInstanceOf[ODataStreamOffset]
    val latest = end.asInstanceOf[ODataStreamOffset]

    var startPartitionValue: Int = 0
    var endPartitionValue: Int = 0

    if (
      startOffset.value == latest.value && startOffset.startValue == latest.startValue
    ) {
      // below will be commented because of the new condition where we are not updated the latest record counts and total page in start offset
      // startPartitionValue = startOffset.currentPage + 1
      startPartitionValue =
        if (
          (latest.pagesLimit > 0) && ((startOffset.totalPage != latest.totalPage) || (startOffset.recordsCount > latest.recordsCount))
        )
          if ((latest.currentPage - latest.pagesLimit + 1) > 0)
            latest.currentPage - latest.pagesLimit + 1
          else 0
        else startOffset.currentPage + 1
      endPartitionValue = latest.currentPage

    } else {
      startPartitionValue = 0
      endPartitionValue = latest.currentPage
    }
    val startModifiedTimestamp: Timestamp = latest.startValue
    val endModifiedTimestamp: Timestamp = latest.value
    val topLimt: Int = latest.limit

    val startModifiedTimestampFilter: PushedFilter = PushedFilter(
      DefaultODataVariables.INCREMENTAL_FIELD,
      ">=",
      parseValue(startModifiedTimestamp),
      GreaterThanOrEqual(
        DefaultODataVariables.INCREMENTAL_FIELD,
        startModifiedTimestamp
      )
    )
    val endModifiedTimestampFilter: PushedFilter = PushedFilter(
      DefaultODataVariables.INCREMENTAL_FIELD,
      "<",
      parseValue(endModifiedTimestamp),
      LessThan(DefaultODataVariables.INCREMENTAL_FIELD, endModifiedTimestamp)
    )
    val pushedMicroBatchFilter: Array[PushedFilter] =
      Array(startModifiedTimestampFilter, endModifiedTimestampFilter)

    this._pushedFilter = scanJob.pushedFilter match {
      case Some(filter) =>
        Some(
          filter.filter(
            _.column != DefaultODataVariables.INCREMENTAL_FIELD
          ) ++ pushedMicroBatchFilter
        )
      case None => Some(pushedMicroBatchFilter)
    }

    val rangeOfPartitions: Range = (startPartitionValue to endPartitionValue)

    log.info(
      s"INFO: Creating ${startPartitionValue} to ${endPartitionValue} OData Input Partitions for batch for entity ${scanJob.tableName} "
    )
    println(
      s"INFO: Creating ${startPartitionValue} to ${endPartitionValue} OData Input Partitions for batch for entity ${scanJob.tableName} "
    )

    rangeOfPartitions
      .map(paritionNum => {
        new ODataInputPartition(paritionNum, topLimt)
          .asInstanceOf[InputPartition]
      })
      .toArray

  }

  override def createReaderFactory(): PartitionReaderFactory = {
    this
  }

  /** Reader created at each executor to fetch the data
    */
  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val odataParition: ODataInputPartition =
      partition.asInstanceOf[ODataInputPartition]

    new ODataParitionReader(
      scanJob.copy(pushedFilter = this._pushedFilter),
      odataParition
    )
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {}

  /** This method fetches the existing offset from checkpoint source folder else
    * create one and insert into the checkpoint location
    */

  private def getInitialOffset(): ODataStreamOffset = {

    val metadataLog = new ODataInitialOffsetWriter(
      SparkSession.getActiveSession.get,
      checkpointLocation
    )

    val initialoffset = metadataLog.get(0).getOrElse {
      val offset =
        ODataStreamOffset.getInitialOffsetFromValue(scanJob.earliestTime)
      metadataLog.add(0, offset)
      offset
    }
    println("INFO: initializing the stream source with initialoffset")
    initialoffset

  }

}
