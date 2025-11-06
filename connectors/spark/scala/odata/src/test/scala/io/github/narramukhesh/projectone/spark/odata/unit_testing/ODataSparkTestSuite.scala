package io.github.narramukhesh.projectone.spark.odata.test.unit_testing

import org.scalatest.funsuite._
import org.scalatest.Assertions._
import org.scalatest.matchers._
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import io.github.narramukhesh.projectone.spark.odata.odataEngine.ODataConnectionSpec
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import io.github.narramukhesh.projectone.spark.odata.exception.RequiredOptionsNotProvidedException
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.connector.expressions.Transform
import java.util.{HashMap => JavaMap}
import java.util.{Map => JMap}
import io.github.narramukhesh.projectone.spark.odata.odatahelper._
import org.apache.spark.sql.sources.{IsNotNull, LessThan}
import io.github.narramukhesh.projectone.spark.odata._
import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.connector.read.InputPartition

class ODataSparkTestSuite extends AnyFunSuite with MockitoSugar {
  val connectionspec = mock[ODataConnectionSpec]
  val tableName = Some("Account")
  val schema = ODataSchema(
    Array(
      ODataColumn("field1", "field1","Edm.Int32"),
      ODataColumn("field2", "field2","Edm.String")
    )
  )
  when(connectionspec.instanceUrl).thenReturn("https://something.crm.com/0/odata/")
  when(connectionspec.access_token).thenReturn("xxxxxx")
  when(connectionspec.readConnectionTimeout).thenReturn(Some(1000))
  when(connectionspec.connectionTimeout).thenReturn(Some(10000))

  test(
    "test the odata spark custom data source table provider with no properties to getTable function"
  ) {

    val odataTableProvider = new ODataSource()
    assertThrows[RequiredOptionsNotProvidedException] {
      odataTableProvider.getTable(
        schema = new StructType(),
        partitioning = Array[Transform](),
        properties = CaseInsensitiveStringMap.empty()
      )
    }
  }

  test(
    "test the odata spark custom data source table provider with properties to getTable function"
  ) {

    val odataTableProvider = new ODataSource()
    val credentials = new JavaMap[String, String]()
    credentials.put("clientID", "xxxx")
    credentials.put("clientSecret", "xxxxx")
    credentials.put("instanceUrl", "xxx.com")
    credentials.put("identityUrl", "xxxxx.ca")
    val odatatable = odataTableProvider.getTable(
      schema = new StructType(),
      partitioning = Array[Transform](),
      properties = new CaseInsensitiveStringMap(credentials)
    )

    assert(odatatable.isInstanceOf[ODataTable])
  }

  test(
    "test the odata spark custom data source scan builder push filter test"
  ) {

    val scanJob = ODataScanJob(
      odataSpec = connectionspec,
      tableName = tableName,
      schema = schema
    )

    val odatascanProvider = new ODataScanBuilder(scanJob = scanJob)
    val notpushedfilter = odatascanProvider.pushFilters(
      Array(LessThan("field1", 1), IsNotNull("field1"))
    )
    val fushedfilter = odatascanProvider.pushedFilters()

    // as in our implementation only <=,>=,=,<,> are applicable, so not null filter shouldn't be pushed
    assert(notpushedfilter.sameElements(Array(IsNotNull("field1"))))

    // should check for lessthan filter
    assert(fushedfilter.sameElements(Array(LessThan("field1", 1))))

  }

  test("test the odata spark stream offset") {

    val streamOffset = ODataStreamOffset.fromJson(value =
      "{\"version\":2,\"field\":\"ModifiedOn\",\"offset\":1,\"total_pages\":0,\"current_page\":0,\"rows_per_page\":0,\"start_value\":1,\"limit\":0}"
    )

    // check the returned type of the streamoffset
    assert(streamOffset.isInstanceOf[ODataStreamOffset])

    val currentTimestampEpoch = Instant.now().toEpochMilli

    val streamoffsetTestTwo =
      new ODataStreamOffset(
        value = new Timestamp(currentTimestampEpoch),
        startValue = new Timestamp(currentTimestampEpoch),
        totalPage = 0,
        currentPage = 0,
        rowsPerPage = 0,
        recordsCount=0,
        limit = 0
      )
    // as spark uses the .json() from stream offset it should matches with json pattern specified
    assert(
      streamoffsetTestTwo == ODataStreamOffset.fromJson(value =
        s"""{"version":3,"field":"ModifiedOn","offset":${currentTimestampEpoch},"total_pages":0,"current_page":0,"rows_per_page":0,"start_value":${currentTimestampEpoch},"records_count":0,"pages_limit":0,"limit":0}"""
      )
    )
  }

  test("test the odata spark data source planning partitions") {

    val scanJob = ODataScanJob(
      odataSpec = connectionspec,
      tableName = tableName,
      schema = schema,
      rowsPerPage = Some(10)
    )

    when(
      connectionspec.get(
        url = ArgumentMatchers.eq("https://something.crm.com/0/odata/Account"),
        headers = ArgumentMatchers.any(),
        params = ArgumentMatchers.eq(
          Some(
            Map("$count" -> "true", "$top" -> "0")
          )
        ),
        exceptionString = ArgumentMatchers.any(),
        readConnectionTimeout = ArgumentMatchers.any(),
        connectionTimeout = ArgumentMatchers.any()
      )
    ).thenReturn("{\"@odata.count\":12,\"value\":[]}")

    val odatascan = new ODataScan(scanJob = scanJob)

    val odatapartitions = odatascan.planInputPartitions()

    // checking the 12 elements is divided into two partition with each partition max length of 10
    assert(odatapartitions.length == 2)
    assert(odatapartitions(0).isInstanceOf[InputPartition])
    assert(
      odatapartitions(1).asInstanceOf[ODataInputPartition].partitionNum == 1
    )
    assert(
      odatapartitions(1)
        .asInstanceOf[ODataInputPartition]
        .elementsToFetch == 10
    )
  }

}
