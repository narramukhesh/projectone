// Databricks notebook source
// DBTITLE 1,Sample Connector Testing
package com.exinity.connector.test
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.connector.catalog.{Table,TableProvider,TableCapability,SupportsRead,SupportsMetadataColumns,MetadataColumn, SupportsWrite}
import org.apache.spark.sql.connector.read.{Scan,ScanBuilder, Batch, InputPartition, PartitionReader, PartitionReaderFactory, SupportsPushDownFilters}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo,BatchWrite,Write,DataWriter,DataWriterFactory,WriteBuilder,WriterCommitMessage,PhysicalWriteInfo,SupportsTruncate}
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters.setAsJavaSetConverter
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.catalyst.json.JacksonGenerator
import java.util
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.catalyst.json.{JacksonGenerator, JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import java.nio.charset.{Charset, StandardCharsets}
import java.util.Date
import org.apache.hadoop.mapreduce.{TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.io.{SparkHadoopWriterUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.internal.SQLConf

case class TestPartition() extends InputPartition{

}
case class TestTableCommitMessage(partitionId:Int,taskId:Long,noRecords:Int) extends WriterCommitMessage{
  override def toString()={
    s"TestTableCommitMessage{partitionId=${partitionId}, taskId=${taskId}}, No. of Records=${noRecords}"
  }
}
case class PushedFilter(attr:String,value:Any,filter:Filter,op:String){

}
case class MetadataField(desc:String,dType:DataType) extends MetadataColumn {
  override def name()= desc
  override def dataType() =dType
}


case class TestTable() extends Table with SupportsRead with SupportsWrite with SupportsPushDownFilters with SupportsMetadataColumns with ScanBuilder with Scan with Batch with PartitionReaderFactory with PartitionReader[InternalRow] with BatchWrite  {

  var _pushedFilters:Array[PushedFilter]=Array()
  override def schema={
    StructType(Array(StructField("id",IntegerType)))
  }

  private var curIndex = 0
  private var info:LogicalWriteInfo = null
  private var physicalinfo:PhysicalWriteInfo=null
  private var conf:Configuration=null
  private var sqlConf:SQLConf=null
  private lazy val dataRows:Array[Seq[Any]] = {
    val columns = Array("id","_id","_name")
    val colIndex:Array[(Int,Any)] = _pushedFilters.take(1).map(x=>(columns.indexOf(x.attr),x.value)).toArray
    println(colIndex.toSeq)
    val data = Array(Seq(1,1,"name"),Seq(2,2,"id"))

    if (_pushedFilters.length==0) data else data.filter(x=>x(colIndex(0)._1)==colIndex(0)._2)
    
  }
  override def next(): Boolean = {
    curIndex < dataRows.length
  }
  override def metadataColumns:Array[MetadataColumn]= {Array(MetadataField("_id",IntegerType),MetadataField("_name",StringType))}

  override def pushFilters(filters:Array[Filter]):Array[Filter]={
    val pushFilter = filters.map(x=> x match {
      case EqualTo(attr, value) =>
          Some(PushedFilter(attr, value, EqualTo(attr, value), "="))
      case _=>None
    }).filter(_.nonEmpty).map(_.get).toArray
    val filterApplied=pushFilter.map(_.filter).toArray
    val notAppliedFilter=filters.filter(x=> !filterApplied.contains(x))
    _pushedFilters ++=pushFilter
    println(s"not applied ${_pushedFilters}")
    notAppliedFilter
  }
  override def pushedFilters(): Array[Filter] = {
    println(s"applied ${_pushedFilters.toArray}")
    _pushedFilters.map(_.filter).toArray

  }

  override def get(): InternalRow = {
    val row = dataRows(curIndex)
    curIndex += 1
    InternalRow.apply(row: _*)
  }
  override def close(): Unit = {}
  override def build():this.type={
    this
  }
  override def readSchema(): StructType = schema
  override def toBatch(): this.type={
    this
  }
  override def planInputPartitions():Array[InputPartition]={
    Array(TestPartition().asInstanceOf[InputPartition])
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    this
  }

  override def createReader(parititon:InputPartition):PartitionReader[InternalRow]={this}
  override def name():String={
    "SampleTest"
  }
  override def capabilities():java.util.Set[TableCapability]={
    Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE).asJava
  }
  override def newScanBuilder(options: CaseInsensitiveStringMap) :ScanBuilder ={
    this
  }
  // override def build():Write ={
  //   this
  // }
  

  override def newWriteBuilder(info:LogicalWriteInfo):WriteBuilder={
    this.info=info
    val sparkSession = SparkSession.active
    this.conf = sparkSession.sessionState.newHadoopConfWithOptions(this.info.options().asCaseSensitiveMap.asScala.toMap)
    this.sqlConf=sparkSession.sessionState.conf
    TableWriteBuilder(this)
  }
  

  override def createBatchWriterFactory(info:PhysicalWriteInfo):DataWriterFactory={
    this.physicalinfo=info
    println(this.info.schema)
    println(this.sqlConf.sessionLocalTimeZone)
    val parsedOptions=new JSONOptions(
      this.info.options().asCaseSensitiveMap.asScala.toMap,
      this.sqlConf.sessionLocalTimeZone,
      this.sqlConf.columnNameOfCorruptRecord)
    TestTableWriterFactory(this.info.options().asCaseSensitiveMap.asScala.toMap,this.info.schema,this.sqlConf,this.physicalinfo,this.conf,parsedOptions:JSONOptions)
    }
  
  
  
  override def commit(messages:Array[WriterCommitMessage])={
    for (i<-messages){
      println(i)
    }
  }
  override def abort(messages:Array[WriterCommitMessage])={
    for (i<-messages){
      println(i)
    }
    }
}

case class TestTableWriterFactory(options:Map[String,String],schema:StructType,sqlConf:SQLConf,physicalinfo:PhysicalWriteInfo,conf:Configuration,parsedOptions:JSONOptions) extends DataWriterFactory{
  private[this] val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date)
  @transient private lazy val jobId = SparkHadoopWriterUtils.createJobID(jobTrackerID, 0)
  private def createTaskAttemptContext(
      partitionId: Int,
      realTaskId: Int): TaskAttemptContextImpl = {
    val taskId = new TaskID(jobId, TaskType.MAP, partitionId)
    val taskAttemptId = new TaskAttemptID(taskId, realTaskId)
    // Set up the configuration object
    val hadoopConf = this.conf
    hadoopConf.set("mapreduce.job.id", jobId.toString)
    hadoopConf.set("mapreduce.task.id", taskId.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)
    println(taskId,jobId,jobTrackerID)

    new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
  }
  override def createWriter(partitionId:Int,taskId:Long):TestTableWriter={
     val taskAttemptContext = createTaskAttemptContext(partitionId, taskId.toInt & Int.MaxValue)
     //println(this.sqlConf.sessionLocalTimeZone) 
     TestTableWriter(this.schema,partitionId,taskId,taskAttemptContext,parsedOptions)
  }

}
case class TestTableWriter(schema:DataType,partitionId:Int,taskId:Long,taskContext:TaskAttemptContextImpl,parsedOptions:JSONOptions) extends DataWriter[InternalRow]{
  var records:Array[InternalRow]=Array()
  override def write(record:InternalRow)={records++=Array(record)}
  override def commit()={
    writeData()
    TestTableCommitMessage(partitionId,taskId,records.length)

  }
  override def abort()={
    throw new Exception("Something failed write failed")
  }
  override def close()={

  }
  def writeData()={
    val encoding =  StandardCharsets.UTF_8
    val path = "/Workspace/Users/admin_narra@gotmyapp.onmicrosoft.com/connectors/test.json"
    println(path)
    val writer = CodecStreams.createOutputStreamWriter(taskContext, new Path(path), encoding)

  // create the Generator without separator inserted between 2 records
  val gen = new JacksonGenerator(schema, writer,parsedOptions)
  print(records)
  for (row<-records){
    gen.write(row)
    gen.writeLineEnding()
  }
    gen.close()
    writer.close()


  }
}
case class TableWriteBuilder(table:TestTable) extends WriteBuilder with SupportsTruncate{
// override def build():Write ={
//     this
//   }
  override def truncate():WriteBuilder={
    this
  }

  // override def toBatch(): BatchWrite={
  //   this.table
  // }

  override def buildForBatch(): BatchWrite={
    this.table
  }
}

class TestDataSource extends DataSourceRegister with TableProvider{
  override def inferSchema(options: CaseInsensitiveStringMap):StructType={
    null
  }
  override def shortName = "test"
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {    
    TestTable()
  }
}

// COMMAND ----------

import org.apache.hadoop.fs.{Path,FileSystem}
/Workspace/Users/admin_narra@gotmyapp.onmicrosoft.com/connectors/03-HTTP API Data Source Testing/spot_api.yaml
val path=new Path("file:/Workspace/Users/admin_narra@gotmyapp.onmicrosoft.com/connectors/03-HTTP API Data Source Testing/test.json")

// COMMAND ----------

// MAGIC %sh
// MAGIC cat /Workspace/Users/admin_narra@gotmyapp.onmicrosoft.com/connectors/03-HTTP API Data Source Testing/test.json

// COMMAND ----------

val fs=FileSystem.get(spark.sparkContext.hadoopConfiguration)

// COMMAND ----------

fs.exists(path)

// COMMAND ----------

val f=fs.open(path)

// COMMAND ----------

f.close()

// COMMAND ----------

val f=fs.create(path)

// COMMAND ----------

"{'id':1}".getBytes

// COMMAND ----------

// MAGIC %python
// MAGIC spark.createDataFrame([{"a":1},{"a":2},{"a":3}],schema="a integer").write.format("csv").save("/Workspace/Users/admin_narra@gotmyapp.onmicrosoft.com/connectors/03-HTTP API Data Source Testing/c.csv")

// COMMAND ----------

f.write("{'id':1}".getBytes)

// COMMAND ----------

f.close()

// COMMAND ----------

fs.makeQualified(path)

// COMMAND ----------

// MAGIC %sh
// MAGIC ls "/Workspace/Users/admin_narra@gotmyapp.onmicrosoft.com/connectors/03-HTTP API Data Source Testing/test.json"

// COMMAND ----------

import org.apache.commons.io.IOUtils
val data=new String(IOUtils.toByteArray(f),"utf-8")

// COMMAND ----------

import org.apache.spark.sql.execution.datasources.v2._

// COMMAND ----------

import org.apache.spark.sql.functions._
val df=spark.read.format("com.exinity.connector.test.TestDataSource").load().withColumn("som",col("_id")).filter("som = 1").select("id")
df.write.format("com.exinity.connector.test.TestDataSource").mode("append").save()

// COMMAND ----------

// MAGIC %python
// MAGIC import pyspark.sql.functions as F
// MAGIC spark.read.format("com.exinity.connector.test.TestDataSource").load().withColumn("som",F.col("_id")).filter("som = 1").write.format("com.exinity.connector.test.TestDataSource").mode("append").save()