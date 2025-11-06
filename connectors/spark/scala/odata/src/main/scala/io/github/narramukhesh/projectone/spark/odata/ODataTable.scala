package io.github.narramukhesh.projectone.spark.odata

import org.apache.spark.sql.connector.catalog.{Table, SupportsRead}
import java.util
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types.{StructType, DecimalType}
import scala.collection.JavaConverters.setAsJavaSetConverter
import org.apache.spark.sql.catalyst.expressions.{
  EqualTo => CatEqualTo,
  Not => NotEqualTo,
  GreaterThan => CatGreaterThan,
  GreaterThanOrEqual => CatGreaterThanOrEqual,
  LessThan => CatLessThan,
  LessThanOrEqual => CatLessThanOrEqual,
  Expression
}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.sources._
import io.github.narramukhesh.projectone.spark.odata.odatahelper._
import org.apache.spark.internal.Logging
import io.github.narramukhesh.projectone.spark.odata.odataEngine.{
  ODataClient,
  ODataConnectionSpec
}
import io.github.narramukhesh.projectone.spark.odata.exception.ODataFilterException

/** This is Table representation of the odata entity which spark use to
  * represent the dataframe
  *
  * @param odataSpec:
  *   OData Connection Specifications
  * @param tableName:
  *   OData Entity Name
  * @param rowsPerPage:
  *   No. of rows per page of fetch
  * @param maxPagesToProcess:
  *   No. of pages to be processed at one batch only applicable for micro-batch
  */
case class ODataTable(
    odataSpec: ODataConnectionSpec,
    tableName: Option[String],
    fieldsToBeSelected: Option[Array[String]],
    rowsPerPage: Option[Int] = Some(DefaultODataVariables.ROWS_PER_FETCH),
    maxPagesToProcess: Option[Int] = Some(
      DefaultODataVariables.MAX_PAGES_PER_BATCH
    ),
    earliestTime: Option[Long] = Some(DefaultODataVariables.EARLIEST_TIME),
    predicates: Option[Array[String]] = None
) extends Table
    with SupportsRead
    with Logging
    with ODataHelper {

  private lazy val _schema: ODataSchema = {

    log.info(s"INFO: schema is inferred from odata schema")
    val odataClient = new ODataClient(odataSpec)
    val odataInferSchema = odataClient.getSchema(tableName)

    if (fieldsToBeSelected.isDefined && fieldsToBeSelected.get.size > 0) {
      log.info(
        s"INFO: schema will be pruned to column specified in option ${fieldsToBeSelected}"
      )
      ODataSchema(
        odataInferSchema.columns.filter(field =>
          fieldsToBeSelected.get.contains(field.name)
        )
      )
    } else {
      odataInferSchema
    }
  }

  /** Currently only two read implementation which are batch, micro-batch
    *
    * @return
    */
  override def capabilities(): java.util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ, TableCapability.MICRO_BATCH_READ).asJava
  }

  /** Defines the name of this table
    *
    * @return
    */
  override def name(): String = {
    if (tableName.isDefined) {
      tableName.get
    } else {
      "ODataDataSource"
    }
  }

  /** Defines the schema of the table which is inferred from source everytime
    *
    * @return
    */
  override def schema(): StructType = _schema.sparkSchema

  /** Gets the parsed expression value from catalog parsed expression
    *
    * @return
    */

  def parseCatalogValue(value: Any): Any = {
    value match {
      case v: org.apache.spark.unsafe.types.UTF8String => v.toString
      case v: Integer                                  => v
      case v: DecimalType                              => v
      case _                                           => value.toString
    }
  }

  /** This method parses the filter and converts into the pushed filter
    *
    * @return
    */
  def parseFilter(
      attribute: String,
      value: Any,
      rawFilter: Filter,
      operator: String
  ): Option[PushedFilter] = {
    val column_with_schema = _schema.columns.find(_.name == attribute.toString)
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

  /** parses the filter condition expression and converts into the pushed filter
    *
    * @return
    */
  def parseFilterExpression(expr: String): Option[PushedFilter] = {
    try {
      val conditionExpr = CatalystSqlParser.parseExpression(expr)
      conditionExpr match {
        case CatEqualTo(attr, value) => {
          val valExpr = parseCatalogValue(value.eval())
          val attribute = attr.toString.replace("'", "")
          parseFilter(attribute, valExpr, EqualTo(attribute, valExpr), "=")
        }
        case CatLessThan(attr, value) => {
          val valExpr = parseCatalogValue(value.eval())
          val attribute = attr.toString.replace("'", "")
          parseFilter(attribute, valExpr, LessThan(attribute, valExpr), "<")
        }
        case CatGreaterThan(attr, value) => {
          val valExpr = parseCatalogValue(value.eval())
          val attribute = attr.toString.replace("'", "")
          parseFilter(attribute, valExpr, GreaterThan(attribute, valExpr), ">")
        }
        case CatGreaterThanOrEqual(attr, value) => {
          val valExpr = parseCatalogValue(value.eval())
          val attribute = attr.toString.replace("'", "")
          parseFilter(
            attribute,
            valExpr,
            GreaterThanOrEqual(attribute, valExpr),
            ">="
          )
        }
        case CatLessThanOrEqual(attr, value) => {
          val valExpr = parseCatalogValue(value.eval())
          val attribute = attr.toString.replace("'", "")
          parseFilter(
            attribute,
            valExpr,
            LessThanOrEqual(attribute, valExpr),
            "<="
          )
        }
        case NotEqualTo(child: Expression) => {
          child match {
            case CatEqualTo(attr, value) => {
              val valExpr = parseCatalogValue(value.eval())
              val attribute = attr.toString.replace("'", "")
              parseFilter(attribute, valExpr, EqualTo(attribute, valExpr), "<>")
            }
            case _ =>
              throw new ODataFilterException(
                s"Provided ${expr} Expression is not supported"
              )
          }
        }

        case _ =>
          throw new ODataFilterException(
            s"Provided ${expr} Expression is not supported"
          )
      }
    } catch {
      case e: Throwable =>
        throw new ODataFilterException(
          s"Error caused while parsing the provided filter expression with error ${e}"
        )
    }

  }

  /** Scan builder for logical planning phase
    */
  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    log.info(s"INFO: OData Table from spark builds the scan for datasource")
    new ODataScanBuilder(
      new ODataScanJob(
        tableName = tableName,
        rowsPerPage = rowsPerPage,
        schema = _schema,
        odataSpec = odataSpec,
        maxPagesToProcess = maxPagesToProcess,
        selectedColumn = fieldsToBeSelected,
        earliestTime = earliestTime,
        pushedFilter = predicates match {
          case Some(filter) => {
            val filters = filter
              .filter(x => x.replace(" ", "").length > 0)
              .map(x => parseFilterExpression(x))
              .filter(_.nonEmpty)
              .map(_.get)
            log.info(
              s"INFO: OData Predicates to be pushdown with predicates ${filters}"
            )
            if (filters.toArray.length > 0) Some(filters.toArray) else None
          }
          case None => None
        }
      )
    )
  }

}
