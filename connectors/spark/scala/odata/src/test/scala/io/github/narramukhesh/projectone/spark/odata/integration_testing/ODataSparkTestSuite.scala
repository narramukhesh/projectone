package io.github.narramukhesh.projectone.spark.odata.test.integration_testing

import org.scalatest.Assertions._
import org.scalatest.funsuite.{FixtureAnyFunSuite, AnyFunSuite}
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import java.net.{InetSocketAddress, HttpURLConnection}
import com.sun.net.httpserver.{HttpServer, HttpHandler, HttpExchange}
import org.apache.spark.sql.types._

class ODataSparkIntegrationTest extends FixtureAnyFunSuite with Matchers {

  case class FixtureParam(spark: SparkSession)

  def withFixture(test: OneArgTest) = {
    // creating the fixture which creates the mock http server for odata end-point and spark session builder which is created and
    // destroyed at the end of the tests
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val endpoint = new InetSocketAddress(8867)
    val server = HttpServer.create(endpoint, 0)
    val fixtureparam = FixtureParam(spark)
    val datahandler = new HttpHandler() {

      def handle(exchange: HttpExchange) = {
        // this handle returns the same data of 3 rows with no extra param filteration
        val response =
          "{\"@odata.count\":3,\"value\":[{\"field1\":1,\"field2\":2},{\"field1\":7,\"field2\":4},{\"field1\":2,\"field2\":4}]}"
            .getBytes()
        exchange.sendResponseHeaders(
          HttpURLConnection.HTTP_OK,
          response.length
        );
        exchange.getResponseBody().write(response);
        exchange.close();
      }
    }
    val tokenhandler = new HttpHandler() {

      // Handle for the token generation
      def handle(exchange: HttpExchange) = {
        val response = "{\"access_token\":\"accesstokenx305\",\"expires_in\":3600}".getBytes()
        exchange.sendResponseHeaders(
          HttpURLConnection.HTTP_OK,
          response.length
        );
        exchange.getResponseBody().write(response);
        exchange.close();
      }
    }

    val schemahandler = new HttpHandler() {
      // handle for schema generation process
      def handle(exchange: HttpExchange) = {
        val response = """<?xml version="1.0" encoding="utf-8"?>
            <edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
                <edmx:DataServices>
                    <Schema Namespace="Terrasoft.Configuration.OData" xmlns="http://docs.oasis-open.org/odata/ns/edm">
                        <EntityType Name="Account">
                            <Key>
                                <PropertyRef Name="field1" />
                            </Key>
                            <Property Name="field1" Type="Edm.Int32" />
                            <Property Name="field2" Type="Edm.Int32" />
                        </EntityType>
                    </Schema>
                </edmx:DataServices>
            </edmx:Edmx>""".getBytes()
        exchange.sendResponseHeaders(
          HttpURLConnection.HTTP_OK,
          response.length
        );
        exchange.getResponseBody().write(response);
        exchange.close();

      }
    }

    server.createContext("/0/odata/Account", datahandler)
    server.createContext("/0/odata/$metadata", schemahandler)
    server.createContext("/connect/token", tokenhandler)

    server.start()

    try {
      withFixture(test.toNoArgTest(fixtureparam))
    } finally {
      server.stop(0)
      spark.stop()
    }
  }

  test(
    "this test reads the data from odata which executes in batch ingestion mode"
  ) { f =>
    // reading the batch table and seeing returned rows matching or not
    val odatadata = f.spark.read
      .format("odata")
      .options(
        Map(
          "clientID" -> "xxxx",
          "clientSecret" -> "xxxx",
          "identityUrl" -> "http://localhost:8867",
          "instanceUrl" -> "http://localhost:8867/0/odata/"
        )
      )
      .load("Account")

    assert(
      odatadata.schema == StructType(
        Array(
          StructField("field1", IntegerType),
          StructField("field2", IntegerType)
        )
      )
    )
    assert(odatadata.count() == 3)
  }

  test(
    "this test reads the data from odata which executes in micro-batch ingestion mode"
  ) { f =>
    val odatadata = f.spark.readStream
      .format("odata")
      .options(
        Map(
          "clientID" -> "xxxx",
          "clientSecret" -> "xxxx",
          "identityUrl" -> "http://localhost:8867",
          "instanceUrl" -> "http://localhost:8867/0/odata/"
        )
      )
      .load("Account")

    // checking the micro-batch ingestion with no exception and stopping the stream
    noException should be thrownBy {
      val streamQuery =
        odatadata.writeStream.format("memory").queryName("account").start()
      streamQuery.stop()
    }

  }

  test(
    "this test reads the data from odata which executes in batch ingestion mode and prunes the columns by fields_to_be_selected option "
  ) { f =>
    // reading the batch table and seeing returned rows matching or not
    val odatadata = f.spark.read
      .format("odata")
      .options(
        Map(
          "clientID" -> "xxxx",
          "clientSecret" -> "xxxx",
          "identityUrl" -> "http://localhost:8867",
          "instanceUrl" -> "http://localhost:8867/0/odata/",
          "fields_to_be_selected" -> "field2"
        )
      )
      .load("Account")

    assert(
      odatadata.schema == StructType(
        Array(
          StructField("field2", IntegerType)
        )
      )
    )
    assert(odatadata.count() == 3)
  }
}
