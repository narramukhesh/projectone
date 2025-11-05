package io.github.narramukhesh.projectone.spark.odata.odataEngine

import requests._
import org.apache.spark.sql.types._
import scala.xml._
import ujson._

import io.github.narramukhesh.projectone.spark.odata.odatahelper.{
  ODataColumn,
  ODataSchema,
  PushedFilter,
  DefaultODataVariables
}
import io.github.narramukhesh.projectone.spark.odata.exception.{
  ODataRequestException,
  ValueParsingError
}

import java.time.{Instant}

/** This class is just a odata http client interface
  */
trait ODataClientHelper {

  /** This method wrapper for the GET Rest http client
    *
    * @param url
    *   url of endpoint
    * @param headers
    *   headers for the specific endpoint
    * @param params
    *   parameter for the endpoint
    * @param exceptionString
    *   exception string to be printed if any issue
    * @param readConnectionTimeout
    *   read connection timeout for the reading from the endpoint
    * @param connectionTimeout
    *   connection timeout for the initial connection for the endpoint
    * @return
    *   the response string
    */
  def get(
      url: String,
      headers: Option[Map[String, String]] = None,
      params: Option[Map[String, String]] = None,
      exceptionString: Option[String] = None,
      readConnectionTimeout: Int =
        DefaultODataVariables.READ_CONNECTION_TIMEOUT,
      connectionTimeout: Int = DefaultODataVariables.CONNECTION_TIMEOUT
  ): String = {
    val response = requests.get(
      url = url,
      connectTimeout = connectionTimeout,
      readTimeout = readConnectionTimeout,
      params = params.getOrElse(None),
      headers = headers.getOrElse(None)
    )
    if (response.statusCode != 200) {
      throw new ODataRequestException(
        s"${exceptionString.getOrElse("")} ${response}"
      )
    }
    return response.text()
  }

  /** This method wrapper for the POST Rest http client
    *
    * @param url
    *   url of endpoint
    * @param headers
    *   headers for the specific endpoint
    * @param data
    *   post data for the specific endpoint
    * @param exceptionString
    *   exception string to be printed if any issue
    * @param readConnectionTimeout
    *   read connection timeout for the reading from the endpoint
    * @param connectionTimeout
    *   connection timeout for the initial connection for the endpoint
    * @return
    *   the response string
    */
  def post(
      url: String,
      headers: Option[Map[String, String]] = None,
      data: Option[Map[String, String]] = None,
      exceptionString: Option[String] = None,
      readConnectionTimeout: Int =
        DefaultODataVariables.READ_CONNECTION_TIMEOUT,
      connectionTimeout: Int = DefaultODataVariables.CONNECTION_TIMEOUT
  ): String = {

    val requestData = data.getOrElse(Nil)
    val response = requests.post(
      url = url,
      connectTimeout = connectionTimeout,
      readTimeout = readConnectionTimeout,
      data = requestData,
      headers = headers.getOrElse(None)
    )
    if (response.statusCode != 200) {
      throw new ODataRequestException(
        s"${exceptionString.getOrElse("")} ${response}"
      )
    }

    return response.text()
  }
}

/** ODataConnectionSpec class is used to hold the odata credentials used to
  * connect. For connecting to odata, we need the below parameters
  * @param clientId
  *   This is client id for ouath autentication
  * @param clientSecret
  *   This is client id for ouath autentication
  * @param identityService
  *   This is identity service url for autentication and getting the access
  *   token
  * @param instanceUrl
  *   This is the main url where all requests hits with access token
  * @param readConnectionTimeout
  *   read connection timeout for the reading from the endpoint
  * @param connectionTimeout
  *   connection timeout for the initial connection for the endpoint
  */
case class ODataConnectionSpec(
    clientId: String,
    clientSecret: String,
    identityService: String,
    instanceUrl: String,
    readConnectionTimeout: Option[Int] = Some(
      DefaultODataVariables.READ_CONNECTION_TIMEOUT
    ),
    connectionTimeout: Option[Int] = Some(
      DefaultODataVariables.CONNECTION_TIMEOUT
    )
) extends ODataClientHelper {

  /** access_token is attribute is the token which will be requested for every
    * expirty time
    */

  var accessToken: String = ""
  var expiryTime: Long = Instant.now().toEpochMilli

  def getAccessToken = {
    try {
      val url = identityService + "/connect/token"
      val response = post(
        url = url,
        data = Some(
          Map(
            "grant_type" -> "client_credentials",
            "client_id" -> clientId,
            "client_secret" -> clientSecret
          )
        ),
        readConnectionTimeout = readConnectionTimeout.get,
        connectionTimeout = connectionTimeout.get,
        exceptionString =
          Some("Cannot Retrive the Access Token Falied with error")
      )
      accessToken = ujson.read(response)("access_token").str
      expiryTime = Instant
        .now()
        .plusSeconds(ujson.read(response)("expires_in").num.toLong - 30)
        .toEpochMilli
    } catch {
      case e: Throwable => {
        throw new ODataRequestException(
          s"Error while extracting Access Token Falied with error ${e}"
        )
      }
    }
  }

  def access_token = {
    if ((accessToken.isEmpty) || (Instant.now().toEpochMilli > expiryTime)) {
      getAccessToken
    }
    accessToken
  }

  override def toString(): String = {
    "ODataConnectionSpec(ClientId:[REDACTED],ClientSecret:[REDACTED],identityService:[REDACTED],instanceUrl:[REDACTED])"
  }

}

/** ODataClient class is a wrapper for sending the requests to odata
  * end-point.
  * @param odataSpec
  *   This is the odata connection object
  */
class ODataClient(odataSpec: ODataConnectionSpec) {

  /** This method is a helper which is used to construct the query url for
    * requesting the odata end-point Example: constructQueryUrl("Account")
    * returns "https://something.com/0/odata/Account"
    * @param tableName
    *   which is a optional, if specified it sets the tableName in url
    * @return
    *   String
    */

  def constructQueryUrl(tableName: Option[String]): String = {
    tableName match {
      case Some(table) => odataSpec.instanceUrl + table
      case None        => odataSpec.instanceUrl
    }
  }

  /** This method is a helper which is used to construct the page arguments for
    * requesting the odata end-point, It returns key-value pair for query
    * parameters which is used for filteration, selection fields Example:
    * constructPageExtraArguments(selectionFields=["field1","field2"],filters=[PushedFilter("field1",">=","2")])
    * returns Map("filter"->"field1 ge 2","select"->"field1,field2")
    * @param selectionFields
    *   which is a optional, if specified it selects the specific fields from
    *   page url
    * @param filters
    *   which is a optional, if specified it applies the filter condition
    */

  def constructPageExtraArguments(
      selectionFields: Option[Array[String]] = None,
      filters: Option[Array[PushedFilter]] = None
  ): Option[Map[String, String]] = {

    val selectionArguments: Option[Map[String, String]] =
      selectionFields match {
        case Some(fields) => {
          if (fields.length > 0) {
            Some(Map("$select" -> fields.mkString(",")))
          } else {
            None
          }
        }
        case None => None
      }
    val filtersArguments: Option[Map[String, String]] = filters match {
      case Some(filters_) => {
        if (filters_.length > 0) {
          Some(
            Map("$filter" -> filters_.map(_.filterExpression).mkString(" and "))
          )
        } else {
          None
        }
      }
      case None => None
    }
    (selectionArguments, filtersArguments) match {
      case (Some(selection), Some(filter)) => Some(selection ++ filter)
      case (Some(selection), _)            => Some(selection)
      case (_, Some(filter))               => Some(filter)
      case _                               => None
    }
  }

  /** This method is a helper which is used to get the count of rows for
    * specific entity
    * @param tableName:
    *   EntityName
    * @param filters:
    *   which is a optional, if specified it applies the filter condition
    *
    * It returns count of rows returned after applying the filters to tableName
    * Example:
    * getEntityCount(tableName="Account",filters=[PushedFilter("field1",">=","2")])
    * returns 5
    * @return
    *   the entity count
    */
  def getEntityCount(
      tableName: Option[String] = None,
      filters: Option[Array[PushedFilter]] = None
  ): Int = {
    val url = constructQueryUrl(tableName)
    val headers = Map(
      "Authorization" -> "Bearer ".concat(odataSpec.access_token)
    )
    val params_ = Map("$top" -> "0", "$count" -> "true")
    val params = filters match {
      case Some(filters_) => {
        if (filters_.length > 0) {
          Map(
            "$top" -> "0",
            "$count" -> "true",
            "$filter" -> filters_.map(_.filterExpression).mkString(" and ")
          )
        } else {
          Map("$top" -> "0", "$count" -> "true")
        }
      }
      case None => Map("$top" -> "0", "$count" -> "true")
    }
    println(
      s"INFO: OData Requested page url:$url,parameters:$params for getting entity Count"
    )
    val response = odataSpec.get(
      url = url,
      params = Some(params),
      headers = Some(headers),
      exceptionString = Some("Request Failed because of error"),
      readConnectionTimeout = odataSpec.readConnectionTimeout.get,
      connectionTimeout = odataSpec.connectionTimeout.get
    )

    var parseResponse: ujson.Value = ujson.read("{}")
    try {
      parseResponse = ujson.read(response)
    } catch {
      case _: Throwable =>
        throw new ValueParsingError("Request Response parsing error")
    }

    val entityCount: Int = tableName match {
      case Some(table) =>
        parseResponse("@odata.count").num.asInstanceOf[Int]
      case _ =>
        parseResponse("value").arr.toArray.length.asInstanceOf[Int]
    }
    println(s"INFO: OData data source entity Count:$entityCount")
    entityCount
  }

  /** This method is a helper which is used to get the data specific entity in
    * page-basis, Where index and rowsPerFetch is used to control the data limit
    * from API It returns a array of dictionary/Map of rows. Example:
    * getPageTableData(tableName="Account",pageExtraArguments=Map("filter"->"field1
    * ge 2","select"->"field1,field2"),index=0,rowPerFetch=1) This fetches 1 row
    * at page 0, where in request url looks something like this
    * "https://something.com/0/odata/Account?top=1&skip=0&filter=field1
    * ge 2&select=field1" returns Array(Map("field1"->3,"field2"->4))
    *
    * @param tableName
    *   EntityName
    * @param pageExtraArguments
    *   these are the arguments constructured by the constructPageExtraArguments
    *   method
    * @param index
    *   Page index at which is need to be retreived
    * @param rowsPerFetch
    *   Rows/data fetched per page
    *
    * @return
    *   the array of mapping of data objects
    */

  def getPageTableData(
      tableName: Option[String] = None,
      pageExtraArguments: Option[Map[String, String]] = None,
      index: Int = 0,
      rowsPerFetch: Option[Int] = Some(DefaultODataVariables.ROWS_PER_FETCH)
  ): Array[upickle.core.LinkedHashMap[String, ujson.Value]] = {
    val params: Map[String, String] = {
      pageExtraArguments match {
        case Some(filters) =>
          Map(
            "$top" -> rowsPerFetch.get.toString,
            "$skip" -> (index * rowsPerFetch.get).toString,
            "$orderby" -> s"${DefaultODataVariables.INCREMENTAL_FIELD} asc"
          ) ++ filters
        case None =>
          Map(
            "$top" -> rowsPerFetch.get.toString,
            "$skip" -> (index * rowsPerFetch.get).toString,
            "$orderby" -> s"${DefaultODataVariables.INCREMENTAL_FIELD} asc"
          )
      }
    }

    val url = constructQueryUrl(tableName)
    val headers = Map(
      "Authorization" -> "Bearer ".concat(odataSpec.access_token)
    )
    println(
      s"INFO: OData Requested page url:$url,parameters:$params for getting page data"
    )

    val response = odataSpec.get(
      url = url,
      params = Some(params),
      headers = Some(headers),
      exceptionString = Some("Request Failed because of error"),
      readConnectionTimeout = odataSpec.readConnectionTimeout.get,
      connectionTimeout = odataSpec.connectionTimeout.get
    )

    var parseResponse: ujson.Value = ujson.read("{}")
    try {
      parseResponse = ujson.read(response)
    } catch {
      case _: Throwable =>
        throw new ValueParsingError("Request Response parsing error")
    }

    parseResponse("value").arr.toArray.map(x => x.obj)
  }

  /** This method is a helper which is used to get the data schema for specific
    * entity
    *
    * getSchema(tableName="Account") returns
    * ODataSchema(Array(ODataColumn(name="field1",sourceType="Edm.Int32"),ODataColumn(name="field2",sourceType="Edm.Int32"),ODataColumn(name="field3",sourceType="Edm.DateTimeOffset")))
    *
    * @param tableName
    *   EntityName It returns a specified schema. Example:
    *
    * @return
    *   a `ODataSchema` object
    */
  def getSchema(tableName: Option[String] = None): ODataSchema = {

    var schema_holder = scala.collection.mutable.ArrayBuffer[ODataColumn]()
    if (tableName != None) {
      val url = constructQueryUrl(Some("$metadata"))
      val headers = Map(
        "Authorization" -> "Bearer ".concat(odataSpec.access_token)
      )
      println(
        s"INFO: OData Requested page url:$url for getting schema for table"
      )
      val response = odataSpec.get(
        url = url,
        headers = Some(headers),
        exceptionString = Some("Request Failed because of error"),
        readConnectionTimeout = odataSpec.readConnectionTimeout.get,
        connectionTimeout = odataSpec.connectionTimeout.get
      )
      val schema = XML.loadString(response)

      for (sch <- (schema \\ "EntityType")) {
        if ((sch \ "@Name").text == tableName.getOrElse(None)) {
          var navigations: Array[String] = Array()
          for (navigation <- (sch \\ "NavigationProperty")) {
            navigations +:= (navigation \ "@Name").text
          }
          for (column <- (sch \\ "Property")) {
            if ((column \ "@Type").text != "Edm.Stream") {
              var col_name: String = (column \ "@Name").text
              if (
                (navigations.length > 0) & ((column \ "@Type").text == "Edm.Guid")
              ) {
                col_name = (column \ "@Name").text.replace("Id", "")
                if (navigations.contains(col_name)) {
                  col_name = s"${col_name}/Id"
                }
              }
              schema_holder += new ODataColumn(
                (column \ "@Name").text,
                col_name,
                (column \ "@Type").text
              )
            }
          }
        }
      }

    } else {
      schema_holder += new ODataColumn("name", "name","Edm.String")
      schema_holder += new ODataColumn("kind", "kind","Edm.String")
      schema_holder += new ODataColumn("url", "url","Edm.String")
    }

    new ODataSchema(schema_holder)
  }
}
