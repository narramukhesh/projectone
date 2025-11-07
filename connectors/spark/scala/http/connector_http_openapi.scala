// Databricks notebook source
object ApiMethod extends Enumeration{
  type ApiMethod = Value
  val GET = Value(0,"Get")
  val POST = Value(1,"Post")
  val PUT = Value(2,"Put")
  val DELETE = Value(3,"delete")
}

case class ApiColumn(sourceName:String,
  targetName:String,
  predicate:Boolean,
  required:Boolean,
  method:String){

}


val a=ApiMethod.withName("Get")
a==ApiMethod.POST

// COMMAND ----------

val a= "/api/v2/accounts/{accountId}/balance"
a.contains("{")

// COMMAND ----------

import io.swagger.v3.parser.OpenAPIV3Parser
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.security.{SecurityRequirement,SecurityScheme}
import io.swagger.v3.oas.models.media.{ArraySchema,StringSchema,NumberSchema,IntegerSchema,DateSchema,DateTimeSchema,BooleanSchema,BinarySchema,ObjectSchema,MapSchema};
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.parser.core.models.ParseOptions
import java.util.Objects.requireNonNull;
import scala.collection.JavaConverters._  
import org.apache.spark.sql.types._
import java.util.stream.Collectors;
import io.swagger.v3.oas.models.OpenAPI


case class Predicate(method:PathItem.HttpMethod,placement:String){
}
case class ApiTable(
    select_path:String,
    select_method:PathItem.HttpMethod
)
case class ApiColumn(
    sourceName:String,
    targetName:String,
    sourceType:Schema[_],
    targetType:DataType,
    requiredPredicate:Option[Array[Predicate]],
    optionalPredicate:Option[Array[Predicate]],
    nullable:Boolean,
    sourceStyle:Parameter.StyleEnum
)
{
    override def toString(): String = {
    s"ApiColumn(sourceName:${sourceName},targetName:${targetName},sourceType:${sourceType},targetType:${targetType},requiredPredicate:${requiredPredicate},optionalPredicate:${optionalPredicate},nullable:${nullable})"
  }
}

case class TypeHolder(sparkType:DataType,apiSchema:Schema[_]){

}
object ApiSpec{

  def parse(location: String):OpenAPI = {
    val parseOptions = new ParseOptions()
    parseOptions.setResolveFully(true)
    //println(parseOptions.isResolveFully())
    val result =
      new OpenAPIV3Parser().readLocation(location, null, parseOptions)
    val openapi = result.getOpenAPI();
    requireNonNull(openapi,"openapi is null")
  }

}
class ApiSpec(openApi:OpenAPI) {
    val MIME_JSON:String="application/json";
val OK:String = "200";
    var tables:Map[String,Array[ApiColumn]]=_
    var paths:Map[String,Map[String,Array[PathItem.HttpMethod]]]=_
    var pathSecurityRequirements:Map[String,Map[PathItem.HttpMethod,Array[SecurityRequirement]]]=_
    var security:java.util.List[SecurityRequirement]=_
    var securitySchemas:java.util.Map[String,SecurityScheme]=_
    def this(location:String)={
        this(ApiSpec.parse(location))
        _initialize()
    }
    def getApiSpec()={
      this.openApi
    }
    def _initialize()={
        val filterApis = this.openApi.getPaths().entrySet().asScala.iterator.filter(entry=>filterTemplatePaths(entry.getKey())).filter(entry=>matchJsonOperationResponse(entry.getValue())).toArray;
        this.tables = filterApis.map(entry=>{
          //println(entry.getKey())
          getTableName(entry.getKey())->getColumns(entry.getValue())}).toMap
        this.paths = filterApis.map(entry=>{
          println(entry.getValue().readOperationsMap().keySet().asScala.toArray)
          getTableName(entry.getKey())->Map(entry.getKey()->entry.getValue().readOperationsMap().keySet().asScala.toArray)}).toMap
        this.pathSecurityRequirements = filterApis.map(entry=>getTableName(entry.getKey())->entry.getValue().readOperationsMap().entrySet().asScala.iterator.filter(entry=>entry.getValue().getSecurity()!=null).map(entry=>entry.getKey()->entry.getValue().getSecurity().asScala.toArray).toMap).toMap
        this.security = this.openApi.getSecurity()
        this.securitySchemas = this.openApi.getComponents().getSecuritySchemes()
    }



def isJsonResponse(operation: Operation): Boolean = {
    operation!=null && operation.getResponses().get(OK) != null && operation
      .getResponses()
      .get(OK)
      .getContent() != null && operation
      .getResponses()
      .get(OK)
      .getContent()
      .get(MIME_JSON) != null
  }
def matchJsonOperationResponse(pathItem: PathItem): Boolean = {
    pathItem.readOperations().stream().anyMatch(isJsonResponse)
  }
def getTableName(path: String): String = {
    path.toLowerCase.trim().stripPrefix("/").stripSuffix("/").replace("/", "_")
  }
def filterTemplatePaths(path:String):Boolean={
    (!path.contains("{"))
  }
def getColumns(pathItem: PathItem): Array[ApiColumn] = {
    pathItem
      .readOperationsMap()
      .entrySet()
      .asScala
      .iterator
      .flatMap(entry => getColumn(entry.getKey(), entry.getValue()))
      .toArray
      .distinct
  }
def getColumn(method: PathItem.HttpMethod, operation: Operation) = {
    var schema: Option[Schema[_]] = getResponseSchema(operation)
    var result: Array[ApiColumn] = Array()
    schema match {
      case Some(sc) => {
        val required: Array[String] =
          if (sc.getRequired() != null) sc.getRequired().asScala.toArray
          else Array();
        val tempResult = getSchemaProperties(sc)
          .entrySet()
          .asScala
          .iterator
          .map(s =>{
            val convertType = getSparkType(s.getValue())
            ApiColumn(
              s.getKey(),
              s.getKey(),
              convertType.apiSchema,
              convertType.sparkType,
              None,
              None,
              required.contains(s.getKey()),
              null
            )
          }
          )
          .toArray
        result ++= tempResult
      }
      case _ => None
    }
    schema = getRequestSchema(operation)
    schema match {
      case Some(sc) => {
        val required: Array[String] =
          if (sc.getRequired() != null) sc.getRequired().asScala.toArray
          else Array();
        val resultSchemaCols:Array[String] = result.map(x=>x.targetName)
        val tempResult = getSchemaProperties(sc)
          .entrySet()
          .asScala
          .iterator
          .map(s => {
            val requiredPredicate =
              if (required.contains(s.getKey()))
                Some(Array(Predicate(method, "body")))
              else None
            val optionalPredicate =
              if (!required.contains(s.getKey()))
                Some(Array(Predicate(method, "body")))
              else None
            val name = if (resultSchemaCols.contains(s.getKey())) s"reg_${s.getKey()}" else s.getKey()
            val convertType = getSparkType(s.getValue())
            ApiColumn(
              s.getKey(),
              name,
              convertType.apiSchema,
              convertType.sparkType,
              requiredPredicate,
              optionalPredicate,
              required.contains(s.getKey()),
              null
            )
          })
          .toArray
        result ++= tempResult
      }
      case _ => None
    }

    if (operation.getParameters()!=null){
      val parameters = operation.getParameters()
      val resultSchemaCols:Array[String] = result.map(x=>x.targetName)
      val tempResult = parameters.asScala.iterator.map(s=>{

        var parameter = s
        if (parameter.getName()==null){
          println(parameter)
          if (parameter.get$ref() != null && parameter.get$ref().startsWith("#")) {
            val parameterName=parameter.get$ref().split("/").last
            val reference = this.openApi.getComponents().getParameters().get(parameterName)
            parameter = reference
        }
        }

        val requiredPredicate= if (parameter.getRequired()) Some(Array(Predicate(method,parameter.getIn()))) else None
        val optionalPredicate =  if (!parameter.getRequired()) Some(Array(Predicate(method,parameter.getIn()))) else None
        val name = if (resultSchemaCols.contains(parameter.getName())) s"reg_${parameter.getName()}" else parameter.getName()
        val convertType = getSparkType(parameter.getSchema())
        ApiColumn(
              parameter.getName(),
              name,
              convertType.apiSchema,
              convertType.sparkType,
              requiredPredicate,
              optionalPredicate,
              parameter.getRequired(),
              parameter.getStyle()
            )
      }).toArray
      result ++=tempResult
    }
    result
    
  }
def getSparkType(property: Schema[_]): TypeHolder = {
  if (property==null){
    return TypeHolder(StringType,property)
  }
    property match {
      case arr: ArraySchema => {
        val arrType=getSparkType(arr.getItems())
        TypeHolder(ArrayType(arrType.sparkType),arr.items(arrType.apiSchema))
      }
      case obj: ObjectSchema => {
        val properties = obj.getProperties()
        if (properties != null) {
          val fields:Map[String,TypeHolder] = properties
            .entrySet()
            .asScala
            .iterator
            .map(s => s.getKey()->getSparkType(s.getValue()))
            .toMap
          
          val sparkFields=fields.map(x=>StructField(x._1,x._2.sparkType)).toArray
          //val apiSchemaFields:java.util.Map[String,Schema[_]]=fields.map{case (name,holder)=>name->holder.apiSchema}.toMap.asJava
          
          //TypeHolder(StructType(sparkFields),obj.properties(apiSchemaFields))
          TypeHolder(StructType(sparkFields),obj)
        } else {
          TypeHolder(StringType,obj)
        }
      }
      case map:MapSchema=>{
        if (map.getAdditionalProperties().isInstanceOf[Schema[_]]){
        val mapType = getSparkType(map.getAdditionalProperties().asInstanceOf[Schema[_]])
        if (mapType==null){
            TypeHolder(MapType(StringType,mapType.sparkType),map.additionalProperties(mapType.apiSchema))
        }
        else{
          TypeHolder(StringType,map)
        }
        }
        else{
          TypeHolder(StringType,map)
        }
        
      }
      case int: IntegerSchema => {
        val format = int.getFormat()
        format match {
          case "int64" =>TypeHolder(LongType,int)
          case _ => TypeHolder(IntegerType,int)
      }
      }
      case num: NumberSchema => {
        val format = num.getFormat()
        if (format != null) {
          format match {
            case "float" => TypeHolder(FloatType,num)
            case _       => TypeHolder(DoubleType,num)
          }
        } else {
          TypeHolder(DoubleType,num)
        }
      }
      case bool: BooleanSchema       => TypeHolder(BooleanType,bool)
      case date: DateSchema          => TypeHolder(DateType,date)
      case timestamp: DateTimeSchema => TypeHolder(TimestampType,timestamp)
      case _                         => {
        if (property==null){
        //println(property)
        }
        var pType:String = property.getType()

        if (pType ==null && property.getTypes() !=null && property.getTypes().size()==1){
          pType=property.getTypes().iterator().next()
        }
        if (pType==null){
          if (property.get$ref() != null && property.get$ref().startsWith("#")) {
            val schemaName=property.get$ref().split("/").last
            val reference = this.openApi.getComponents().getSchemas().get(schemaName)
            return getSparkType(reference)
        }
        else if (property.getProperties()!=null){
          pType="object"
        }
        else{
          return TypeHolder(StringType,property)
        }
        }

        pType match{
          case "array"=>{
            val ie=property.getItems()
            if (ie==null){   
              print(property)         
              //println(ie)
            }
        val items = getSparkType(property.getItems())
        TypeHolder(ArrayType(items.sparkType),new ArraySchema().items(items.apiSchema))

      }
      case "string"=>{
        val format = property.getFormat()
        format match {
          case "date" => TypeHolder(DateType,property)
          case "date-time" => TypeHolder(TimestampType,property)
          case _ => TypeHolder(StringType,property)
        }
      }
      case "int" | "integer" =>TypeHolder(IntegerType,property)
      case "float"=>TypeHolder(FloatType,property)
      case "number" => TypeHolder(DoubleType,property)
      case "object" => {
        if (property.getAdditionalProperties().isInstanceOf[Schema[_]]){
        val mapType = getSparkType(property.getAdditionalProperties().asInstanceOf[Schema[_]])
        if (mapType==null){
            TypeHolder(MapType(StringType,mapType.sparkType),property.additionalProperties(mapType.apiSchema))
        }
        else{
          TypeHolder(StringType,property)
        }
        }
        else{
          val properties = property.getProperties()
        if (properties != null) {
          val fields:Map[String,TypeHolder] = properties
            .entrySet()
            .asScala
            .iterator
            .map(s => s.getKey()->getSparkType(s.getValue()))
            .toMap
          
          val sparkFields=fields.map(x=>StructField(x._1,x._2.sparkType)).toArray
          //val apiSchemaFields:java.util.Map[String,Schema[_]]=fields.map{case (name,holder)=>name->holder.apiSchema}.toMap.asJava
          
          //TypeHolder(StructType(sparkFields),obj.properties(apiSchemaFields))
          TypeHolder(StructType(sparkFields),new ObjectSchema().properties(properties))
        } else {
          TypeHolder(StringType,property)
        }


        }

      }
      case _=> {
        val referenceType = this.openApi.getComponents().getSchemas().get(pType)
        if (referenceType!=null){
        val convertType = getSparkType(referenceType)
        TypeHolder(convertType.sparkType,referenceType)
        }
        else{
          TypeHolder(StringType,property)
        }
      }

        }

      }
    }
  }

def getSchemaProperties(schema: Schema[_]) = {
    val properties = if (schema.isInstanceOf[ArraySchema] && schema.getItems() != null) {
      schema.getItems().getProperties()
    } else {
      schema.getProperties()
    }
    if (properties==null) java.util.Collections.emptyMap() else properties
  }
def getResponseSchema(operation: Operation): Option[Schema[_]] = {

    if (
      operation.getResponses().get(OK) != null && operation
        .getResponses()
        .get(OK)
        .getContent() != null && operation
        .getResponses()
        .get(OK)
        .getContent()
        .get(MIME_JSON) != null
    ) {
      Some(
        operation.getResponses().get(OK).getContent().get(MIME_JSON).getSchema()
      )
    } else {
      None
    }

  }
def getRequestSchema(operation: Operation): Option[Schema[_]] = {

    if (
      operation.getRequestBody() != null && operation
        .getRequestBody()
        .getContent() != null && operation
        .getRequestBody()
        .getContent()
        .get(MIME_JSON) != null
    ) {
      Some(operation.getRequestBody().getContent().get(MIME_JSON).getSchema())
    } else {
      None
    }

  }
}


//
//println(tables)


// COMMAND ----------


object AuthType extends Enumeration{
  type AuthType=Value
  val HTTP, APIKEY, OAUTH = Value
}
object AuthSchema extends Enumeration{
  type AuthSchema=Value
  val BEARER, BASIC = Value
}

case class ApiConfig(
  autenticationType:AuthType.AuthType=AuthType.HTTP,
  autenticationSchema:AuthSchema.AuthSchema=AuthSchema.BASIC,
  autenticationBaseUrl:String,
  autenticationClientId:String=null,
  autenticationClientSecret:String=null,
  autenticationBearerToken:String=null,
  autenticationApiKeys:String=null,
  autenticationApiKeyName:String=null,
  autenticationApiKeyHolder:String=null,
  autenticationTokenEndpoint:String=null,
  autenticationRefreshTokenEndpoint:String=null,
  autenticationGrantType:String=null,
  autenticationUserName:String=null,
  autenticationPassword:String=null,
  autenticationUserNameAlias:String=null,
  autenticationPasswordAlias:String=null,
  autenticationforceAuth:Boolean=false,
  autenticationAccessPath:String="access_token"
)
{
  def getApiKeys():Map[String,String]={
    if (this.autenticationApiKeys !=null){ this.autenticationApiKeys.split(",").map(x=>x.split("=")).map(x=>x(0)->x(1)).toMap} else Map()
  }
  def getautenticationGrantType()={
    if (autenticationGrantType==null) "client_credentials" else autenticationGrantType
  }

  def getAuthenticationAccessPath():Array[String]={

    val accessPath=autenticationAccessPath.split("\\.")
    //println(accessPath.toArray)
    accessPath
  }

}

// COMMAND ----------

import scala.collection.mutable.{Map=>MutableMap}
import ujson._

case class Request(
    baseUrl:String,
    targetUrl: String,
    method:PathItem.HttpMethod,
    var headers: Option[Map[String, String]] = None,
    var parameters: Option[Map[String, String]] = None,
    var requestBody: Option[Map[String, String]] = None
) {
  def addHeader(header:String,value:String):this.type={
    this.headers match {
      case Some(headers)=>
      {
        this.headers=Some(headers++Map(header->value))
      }
      case None=> this.headers=Some(Map(header->value))
    }
    return this
  }
  def addRequestBody(key:String,value:String):this.type={
    this.requestBody match {
      case Some(body)=>
      {
        this.requestBody=Some(body++Map(key->value))
      }
      case None=> this.requestBody=Some(Map(key->value))
    }
    return this
  }
  def addParameter(parameter:String,value:String):this.type={
    this.parameters match {
      case Some(parameters)=>
      {
        this.parameters=Some(parameters++Map(parameter->value))
      }
      case None=> this.parameters=Some(Map(parameter->value))
    }
    return this
  }
  def get(
      exceptionString: Option[String] = None,
      readConnectionTimeout: Int = 10000000,
      connectionTimeout: Int = 1000000
  ): ujson.Value = {
    val response = requests.get(
      url = baseUrl+targetUrl,
      connectTimeout = connectionTimeout,
      readTimeout = readConnectionTimeout,
      params = parameters.getOrElse(None),
      headers = headers.getOrElse(None)
    )
    if (response.statusCode != 200) {
      throw new Exception(
        s"${exceptionString.getOrElse("")} ${response}"
      )
    }
    return ujson.read(response.text())
  }
  def post(
      exceptionString: Option[String] = None,
      readConnectionTimeout: Int = 10000000,
      connectionTimeout: Int = 1000000000
  ): ujson.Value = {

    println(requestBody)

    val requestData = requestBody.getOrElse(Nil)
    val response = requests.post(
      url = baseUrl+targetUrl,
      connectTimeout = connectionTimeout,
      readTimeout = readConnectionTimeout,
      data = requestData,
      headers = headers.getOrElse(None)
    )
    if (response.statusCode != 200) {
      throw new Exception(
        s"${exceptionString.getOrElse("")} ${response}"
      )
    }

    return ujson.read(response.text())
  }

  def execute(exceptionString: Option[String] = None,
      readConnectionTimeout: Int = 10000000,
      connectionTimeout: Int = 1000000000)={
        this.method match{
          case PathItem.HttpMethod.POST=> post(exceptionString,readConnectionTimeout,connectionTimeout)
          case PathItem.HttpMethod.GET=> get(exceptionString,readConnectionTimeout,connectionTimeout)
          case _ => throw new Exception("Not Supported Method Type")
      }
}
}

// COMMAND ----------

import java.util.Base64
import scala.annotation.unchecked
class Autentication(apiSpec:ApiSpec,apiConfig:ApiConfig){
  
  def applyAuth(request:Request):Request={

    val url= request.targetUrl
    val method = request.method

    val security = getSecurityMethod(url,method)
    if ((security==null && apiConfig.autenticationType!=null)||(apiConfig.autenticationType!=null && apiConfig.autenticationforceAuth)){
      apiConfig.autenticationType match {
        case AuthType.APIKEY =>{
          val securityScheme=new SecurityScheme()
          securityScheme.setName(this.apiConfig.autenticationApiKeyName)
          securityScheme.setIn(SecurityScheme.In.valueOf(this.apiConfig.autenticationApiKeyHolder))
          return applyApiKeyAuth(request,securityScheme)
        }
        case AuthType.HTTP =>{
          return applyHttpAuth(request,this.apiConfig.autenticationSchema.toString)
        }
        case AuthType.OAUTH => {
          val securityScheme=new SecurityScheme()
          return applyOAuth(request,securityScheme)
        }
      }
      
    }
    else if(security!=null) {
      applySecurityMethod(security,request)
    }
    request
  }

  def applySecurityMethod(security:Array[SecurityRequirement],request:Request)={
    security.foreach{
      requirement=> requirement.forEach{(name,options)=>
      val securitySchema = this.apiSpec.securitySchemas.get(name)
      if(securitySchema !=null){
        securitySchema.getType() match {
          case SecurityScheme.Type.HTTP=>applyHttpAuth(request,securitySchema.getScheme())
          case SecurityScheme.Type.APIKEY=>applyApiKeyAuth(request,securitySchema)
          case SecurityScheme.Type.OAUTH2=>applyOAuth(request,securitySchema)
          case _ => throw new Exception("Not Supported security Requirement")
        }
      }
      }
    }

  }

  def applyHttpAuth(request:Request,securitySchema:String):Request={
    val token = securitySchema.toUpperCase match {
      case "BEARER" => s"Bearer ${this.apiConfig.autenticationBearerToken}"
      case _=> {
        val username = this.apiConfig.autenticationUserName
        val password = this.apiConfig.autenticationPassword
        val finalCreds = s"${username}:${password}"
        val encodedValue = Base64.getEncoder.encodeToString(finalCreds.getBytes("UTF-8"))
        s"${securitySchema} ${encodedValue}"
      }
    }
    request.addHeader("Authorization",token)
  }
  def applyApiKeyAuth(request:Request,securitySchema:SecurityScheme):Request={
    val name = if (securitySchema.getName()==null) this.apiConfig.autenticationApiKeyName else securitySchema.getName()
    if (name==null){
      throw new Exception("Invalid apikey name")
    }
    val apiKeys:Map[String,String]=this.apiConfig.getApiKeys()
    if (apiKeys.isEmpty){
      throw new Exception("Invalid api key")
    }
    val value =apiKeys.getOrElse(name,throw new Exception("Invalid api key name"))

    securitySchema.getIn match {
      case SecurityScheme.In.COOKIE => request.addHeader("Cookie",value)
      case SecurityScheme.In.HEADER=> request.addHeader(name,value)
      case SecurityScheme.In.QUERY=> request.addParameter(name,value)
      case _=> throw new Exception("Invalid security scheme")
      
    }
  }

  def applyOAuth(request:Request,securitySchema:SecurityScheme):Request={
    val oauthflows=securitySchema.getFlows()
    val token = getToken()
    request.addHeader("Authorization",s"Bearer ${token}")
  }
  def getToken():String={
    val tokenUrl = this.apiConfig.autenticationTokenEndpoint
    val baseurl = this.apiConfig.autenticationBaseUrl
    val request = Request(baseUrl=baseurl,method=PathItem.HttpMethod.POST,targetUrl=tokenUrl)
    val grant_type = this.apiConfig.getautenticationGrantType()
    //println(grant_type)
    request.addRequestBody("grant_type",grant_type)
    val userName = if (this.apiConfig.autenticationUserNameAlias!=null) this.apiConfig.autenticationUserNameAlias else "username"
    val password = if (this.apiConfig.autenticationPasswordAlias!=null) this.apiConfig.autenticationPasswordAlias else "password"
    request.addRequestBody(userName,this.apiConfig.autenticationUserName)
    request.addRequestBody(password,this.apiConfig.autenticationPassword)
    if (this.apiConfig.autenticationClientId!=null){
    request.addRequestBody("clientId",this.apiConfig.autenticationClientId)
    }
    if (this.apiConfig.autenticationClientSecret!=null){
    request.addRequestBody("clientSecret",this.apiConfig.autenticationClientSecret)
    }
    val accessTText = request.execute()
    val accessPath:Array[String] = this.apiConfig.getAuthenticationAccessPath()
    println(accessPath)
    val accessToken = accessPath.foldLeft(Option(accessTText:Any)){
      case (Some(m:ujson.Value @unchecked),key:String)=>{
        //println(key)
        //println(m(key))
        Some(m(key))
        }
      case _=>None
    }
    //println(accessToken)
    val token= if (accessToken.isDefined) accessToken.get.asInstanceOf[ujson.Value].str else null
    println(token)
    return token
    
  }

  def getSecurityMethod(url:String,method:PathItem.HttpMethod):Array[SecurityRequirement]={
    if (this.apiSpec.pathSecurityRequirements.contains(url) && this.apiSpec.pathSecurityRequirements.get(url).contains(method)){
      this.apiSpec.pathSecurityRequirements.get(url).get(method)
    }
    else{
      this.apiSpec.security.asScala.toArray
    } 
  }
}

// COMMAND ----------


val a=Array(1,2,3,4)
val b=Map(1->Map(2->Map(3->Map(4->1))))

a.foldLeft(Option(b:Any)){
  case (Some(m:Map[Int,Any]@unchecked),key)=>m.get(key)
  case _=>None
}

// COMMAND ----------

val openapi= new ApiSpec(location="/Workspace/Users/admin_narra@gotmyapp.onmicrosoft.com/connectors/03-HTTP API Data Source Testing/b2t.yaml");

// COMMAND ----------

val apiConfig=ApiConfig(autenticationType=AuthType.OAUTH,
  autenticationBaseUrl="https://api.boxwind.com",
  autenticationTokenEndpoint="/api/v2/signin",
  autenticationGrantType="password",
  autenticationUserName="mukhesh.narra@exinity.com",
  autenticationPassword="u77:Es~Q=rB3VhtK",
  autenticationUserNameAlias="email",
  autenticationPasswordAlias="password",
  autenticationforceAuth=true,
  autenticationAccessPath="accessToken.token")

//Request(baseUrl="")

// COMMAND ----------

val request=Request(baseUrl="https://api.boxwind.com",targetUrl="/api/v2/transactions",method=PathItem.HttpMethod.GET)

// COMMAND ----------

val auth=new Autentication(apiConfig=apiConfig,apiSpec=openapi)

// COMMAND ----------

val requestOP=auth.applyAuth(request)

// COMMAND ----------

requestOP.execute()

// COMMAND ----------

val openapi_w= new ApiSpec(location="/Workspace/Users/admin_narra@gotmyapp.onmicrosoft.com/connectors/03-HTTP API Data Source Testing/b2t.yaml");

// COMMAND ----------

val a_w=openapi_w.tables.get("api_v2_transactions").get

// COMMAND ----------

a_w.filter(x=>x.sourceName=="filter").map(x=>x.targetType)

// COMMAND ----------

val openapi =new ApiSpec(location="/Workspace/Users/admin_narra@gotmyapp.onmicrosoft.com/connectors/03-HTTP API Data Source Testing/datadog.yaml")

// COMMAND ----------

val a=openapi.tables.get("api_v2_authn_mappings").get

// COMMAND ----------

a.map(x=>x.targetName)

// COMMAND ----------

val b=a.getPaths().entrySet().asScala.iterator.filter(x=>x.getKey=="/api/v2/transactions").toArray
b(0).getValue().readOperations().stream().

// COMMAND ----------

val columnsTest=openapi.tables.getOrElse("api_v2_transactions",None).asInstanceOf[Array[ApiColumn]]

// COMMAND ----------

class ApiSpec(){

  
  
  def parse(location:String)={

      val parseOptions = new ParseOptions();
      parseOptions.setResolveFully(true);
      val result = new OpenAPIV3Parser().readLocation(location,null,parseOptions);

      val openapi = result.getOpenAPI();
      openapi
}
}