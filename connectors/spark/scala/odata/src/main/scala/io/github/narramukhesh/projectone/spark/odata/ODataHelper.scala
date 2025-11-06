package io.github.narramukhesh.projectone.spark.odata.odatahelper

/** This Package is used as an Helper for all sub-sequent classed used in the
  * root package
  */

import java.sql.{Timestamp, Date}
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.Filter
import java.time.{
  OffsetDateTime,
  ZoneOffset,
  Instant,
  LocalDateTime,
  ZonedDateTime
}
import java.time.format.{DateTimeFormatterBuilder, DateTimeFormatter}
import java.time.temporal.ChronoField
import io.github.narramukhesh.projectone.spark.odata.exception.{ValueParsingError, TypeNotSupported}

/** This is filter class used for spark PushDown predictive
  *
  * @param column:
  *   Column name
  * @param operator:
  *   operator value
  * @param value:
  *   Column value
  * @param rawFilter:
  *   Actual Filter value
  *
  * Currently only >=,<=,>,<,startsWith,Contains,endswith filter are applied for
  * push down filter
  */
case class PushedFilter(
    column: String,
    operator: String,
    value: String,
    rawFilter: Filter
) {

  def filterExpression: String = {
    operator match {
      case ">="         => s"$column ge $value"
      case "<"          => s"$column lt $value"
      case "<="         => s"$column le $value"
      case ">"          => s"$column gt $value"
      case "="          => s"$column eq $value"
      case "<>"          => s"$column ne $value"
      case "startsWith" => s"startwith($column,$value)"
      case "endsWith"   => s"endswith($column,$value)"
      case "contains"   => s"contains($column,$value)"
      case _            => ""
    }
  }

}

/** This is a interface for the helper class where methods are used in
  * spark-connector in sub-class implementations
  */
trait ODataHelper {

  /** This method is used for parsing the pushed down filter value to odata
    * specific value
    *
    * @param value
    * @param sourceType
    * @return
    */
  def parseValue(value: Any, sourceType: String = "Edm.String") = {
    value match {
      case string: String => {
        if (sourceType == "Edm.Guid") {
          s"${value}"
        } else {
          s"'${value}'"
        }
      }
      case timestamp: Timestamp => {
        val timestampString = s"${value}"
        val originalFormatter = new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 1, 6, true)
          .optionalEnd()
          .toFormatter()

        val localDateTime =
          LocalDateTime.parse(timestampString, originalFormatter)
        val zonedDateTime = localDateTime.atZone(ZoneOffset.UTC)
        val isoFormatter =
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
        val isoFormattedString = zonedDateTime.format(isoFormatter)
        isoFormattedString
      }

      case date: Date => s"${value}"
      case _          => s"${value}"
    }
  }

  /** This method is used for parsing checking the push down filter value type
    * is supported by this connector to push to source
    *
    * @param value
    * @return
    */
  def checkType(fieldType: Any): Boolean = {
    fieldType match {
      case StringType    => true
      case TimestampType => true
      case DoubleType    => true
      case IntegerType   => true
      case _             => false
    }
  }

  /** This method is used for converting the cretio source type to spark target
    * type
    *
    * @param schema
    * @param value
    * @return
    */
  def odataTypeToSparkType(schema: ODataSchema, value: Map[String, Any]) = {
    schema.columns
  }
}

/** This is wrapper class to hold the odata schema
  *
  * @param columns
  */
case class ODataSchema(columns: Iterable[ODataColumn]) {

  lazy val sparkSchema = {
    StructType(columns.map(x => StructField(x.name, x.targetType)).toArray)
  }
}

/** This is wrapper class to hold odata column
  *
  * @param name
  * @param sourceType
  */
case class ODataColumn(name: String,  sourceName:String, sourceType: String) {

  lazy val targetType = {

    sourceType match {
      case "Edm.String"         => StringType
      case "Edm.Int32"          => IntegerType
      case "Edm.DateTimeOffset" => TimestampType
      case "Edm.Decimal"        => DoubleType
      case "Edm.Boolean"        => BooleanType
      case _                    => StringType
    }

  }

  /** This method parse the source value into spark target value
    *
    * @param value
    * @return
    */
  def convertValueToNative(value: Any) = {
    try {
      targetType match {
        case StringType =>
          org.apache.spark.unsafe.types.UTF8String
            .fromString(value.asInstanceOf[java.lang.String])
        case IntegerType =>
          value match {
            case double: Double =>
              value.asInstanceOf[Double].toInt.asInstanceOf[java.lang.Integer]
            case float: Float =>
              value.asInstanceOf[Float].toInt.asInstanceOf[java.lang.Integer]
            case _ => value.asInstanceOf[java.lang.Integer]
          }
        case TimestampType =>
          OffsetDateTime
            .parse(value.asInstanceOf[java.lang.String])
            .toInstant
            .toEpochMilli * 1000
        case DoubleType  => value.asInstanceOf[java.lang.Double]
        case BooleanType => value.asInstanceOf[java.lang.Boolean]
        case _ =>
          throw new TypeNotSupported(
            s"Cannot convert to target Type, only supports  StringType,IntegerType,TimestampType,DoubleType" +
              s"BooleanType"
          )

      }
    } catch {
      case e: Throwable =>
        throw new ValueParsingError(
          "Invalid value, can't be serialized to java type"
        )
    }
  }
}


/** This object hold all the default odata connector variables
    *
    * @param 
    * @return
    */

object DefaultODataVariables{
  val READ_CONNECTION_TIMEOUT: Int = 1800000
  val CONNECTION_TIMEOUT:Int = 100000
  val ROWS_PER_FETCH:Int = 1000
  val MAX_PAGES_PER_BATCH:Int = 4
  val INCREMENTAL_FIELD:String = "ModifiedOn"
  val EARLIEST_TIME:Long = 946684800000L
  val LIMIT_COLUMNS_PUSHDOWN:Int = 105
}
