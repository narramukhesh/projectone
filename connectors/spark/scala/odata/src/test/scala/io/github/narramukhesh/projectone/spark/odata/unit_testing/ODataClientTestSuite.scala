package io.github.narramukhesh.projectone.spark.odata.test.unit_testing

import io.github.narramukhesh.projectone.spark.odata.odataEngine._
import io.github.narramukhesh.projectone.spark.odata.odatahelper.PushedFilter
import org.scalatest.funsuite._
import org.scalatest.Assertions._
import org.scalatest.matchers._
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.apache.spark.sql.sources.GreaterThanOrEqual
import org.mockito.ArgumentMatchers
import io.github.narramukhesh.projectone.spark.odata.odatahelper.ODataSchema

class ODataClientTestSuite extends AnyFunSuite with MockitoSugar {

  val connectionspec = mock[ODataConnectionSpec]

  val odataclient = new ODataClient(connectionspec)

  val tableName = Some("Account")

  val pushfilter =
    PushedFilter("field1", ">=", "2", GreaterThanOrEqual("field1", 2))

  val selectionfields = Some(Array("field2"))

  when(connectionspec.instanceUrl).thenReturn("https://something.crm.com/0/odata/")
  when(connectionspec.access_token).thenReturn("xxxxxx")
  when(connectionspec.readConnectionTimeout).thenReturn(Some(1000))
  when(connectionspec.connectionTimeout).thenReturn(Some(10000))

  test("test the construct query url") {
    // this test checks how the query url constructed
    val returnvalue = odataclient.constructQueryUrl(tableName)

    assert(returnvalue == "https://something.crm.com/0/odata/Account")
  }

  test("test the construct page query arguments") {
    // this test checks the page construction of arguments passed to each page
    val returnvalue = odataclient.constructPageExtraArguments(
      selectionFields = selectionfields,
      filters = Some(Array(pushfilter))
    )

    assert(
      returnvalue.get == Map("$filter" -> "field1 ge 2", "$select" -> "field2")
    )

    val returnvalue2 = odataclient.constructPageExtraArguments(filters =
      Some(Array(pushfilter))
    )

    assert(returnvalue2.get == Map("$filter" -> "field1 ge 2"))

  }

  test("test the entity count for one entity Account") {

    // This test check the entity count
    when(
      connectionspec.get(
        url = ArgumentMatchers.eq("https://something.crm.com/0/odata/Account"),
        headers = ArgumentMatchers.any(),
        params =
          ArgumentMatchers.eq(Some(Map("$count" -> "true", "$top" -> "0"))),
        exceptionString = ArgumentMatchers.any(),
        readConnectionTimeout = ArgumentMatchers.any(),
        connectionTimeout = ArgumentMatchers.any()
      )
    ).thenReturn("{\"@odata.count\":2,\"value\":[]}")

    val returnvalue = odataclient.getEntityCount(tableName)

    assert(returnvalue == 2)

    when(
      connectionspec.get(
        url = ArgumentMatchers.eq("https://something.crm.com/0/odata/Account"),
        headers = ArgumentMatchers.any(),
        params = ArgumentMatchers.eq(
          Some(
            Map("$count" -> "true", "$top" -> "0", "$filter" -> "field1 ge 2")
          )
        ),
        exceptionString = ArgumentMatchers.any(),
        readConnectionTimeout = ArgumentMatchers.any(),
        connectionTimeout = ArgumentMatchers.any()
      )
    ).thenReturn("{\"@odata.count\":1,\"value\":[]}")

    val returnTestvalueTwo = odataclient.getEntityCount(
      tableName = tableName,
      filters = Some(Array(pushfilter))
    )

    assert(returnTestvalueTwo == 1)

  }

  test("test the page data for one entity Account") {

    // This test check the page data for 1 row without skipping and filters
    val expectedValue = "{\"value\":[{\"field1\":1}]}"
    when(
      connectionspec.get(
        url = ArgumentMatchers.eq("https://something.crm.com/0/odata/Account"),
        headers = ArgumentMatchers.any(),
        params = ArgumentMatchers.eq(Some(Map("$top" -> "1", "$skip" -> "0", "$orderby" -> "ModifiedOn asc"))),
        exceptionString = ArgumentMatchers.any(),
        readConnectionTimeout = ArgumentMatchers.any(),
        connectionTimeout = ArgumentMatchers.any()
      )
    ).thenReturn(expectedValue)

    val returnvalue = odataclient.getPageTableData(
      tableName,
      index = 0,
      rowsPerFetch = Some(1)
    )

    // Checking the result
    assert(returnvalue.length == 1)
    assert(returnvalue(0)("field1").num.toInt == 1)
    assert(returnvalue(0).keys.toArray.length == 1)

    // This test checks with filters and select the fields with only getting top 1 row
    val expectedValueTwo = "{\"value\":[{\"field1\":1,\"field2\":2}]}"

    when(
      connectionspec.get(
        url = ArgumentMatchers.eq("https://something.crm.com/0/odata/Account"),
        headers = ArgumentMatchers.any(),
        params = ArgumentMatchers.eq(
          Some(
            Map(
              "$top" -> "1",
              "$skip" -> "0",
              "$orderby" -> "ModifiedOn asc",
              "$filter" -> "field1 ge 1",
              "$select" -> "field1,field2"
            )
          )
        ),
        exceptionString = ArgumentMatchers.any(),
        readConnectionTimeout = ArgumentMatchers.any(),
        connectionTimeout = ArgumentMatchers.any()
      )
    ).thenReturn(expectedValueTwo)

    val returnTestvalueTwo = odataclient.getPageTableData(
      tableName = tableName,
      index = 0,
      rowsPerFetch = Some(1),
      pageExtraArguments =
        Some(Map("$filter" -> "field1 ge 1", "$select" -> "field1,field2"))
    )

    // Checking the result
    assert(returnTestvalueTwo.length == 1)
    assert(returnTestvalueTwo(0)("field2").num.toInt == 2)
    assert(returnTestvalueTwo(0).keys.toArray.length == 2)
    assert(
      returnTestvalueTwo(0).keys.toArray.sameElements(Array("field1", "field2"))
    )

  }

  test("test the schema for entity Account") {

    // test the default schema provided by the method

    val returnvalue = odataclient.getSchema()

    assert(returnvalue.isInstanceOf[ODataSchema])
    assert(returnvalue.columns.toArray.length == 3)

    when(
      connectionspec.get(
        url =
          ArgumentMatchers.eq("https://something.crm.com/0/odata/$metadata"),
        headers = ArgumentMatchers.any(),
        params = ArgumentMatchers.any(),
        exceptionString = ArgumentMatchers.any(),
        readConnectionTimeout = ArgumentMatchers.any(),
        connectionTimeout = ArgumentMatchers.any()
      )
    ).thenReturn("""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
    <edmx:DataServices>
        <Schema Namespace="Terrasoft.Configuration.OData" xmlns="http://docs.oasis-open.org/odata/ns/edm">
            <EntityType Name="Account">
                <Key>
                    <PropertyRef Name="Id" />
                </Key>
                <Property Name="Id" Type="Edm.Int32" />
                <Property Name="Name" Type="Edm.String" />
            </EntityType>
         </Schema>
    </edmx:DataServices>
</edmx:Edmx>""")

    val returnTestvalueTwo = odataclient.getSchema(tableName)

    assert(returnTestvalueTwo.isInstanceOf[ODataSchema])
    assert(returnTestvalueTwo.columns.toArray.length == 2)
    assert(
      returnTestvalueTwo.columns
        .map(x => x.name)
        .toArray
        .sameElements(Array("Id", "Name"))
    )

  }
}
