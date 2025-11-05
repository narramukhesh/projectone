package io.github.narramukhesh.projectone.spark.odata.exception

/** This Exception used for when required options are not defined for the target
  * class
  *
  * @param errMsg
  */
class RequiredOptionsNotProvidedException(errMsg: String)
    extends Exception(errMsg) {}

/** This Exception used for when there is a problems with the odata requests
  * end-point
  *
  * @param errMsg
  */
class ODataRequestException(errMsg: String) extends Exception {}

/** This Exception used for when is a problem with parsing the value returned by
  * odata
  *
  * @param errMsg
  */
class ValueParsingError(errMsg: String) extends Exception {}

/** This Exception used for when is a problem with parsing the odata type not
  * supported
  *
  * @param errMsg
  */
class TypeNotSupported(errMsg: String) extends Exception {}

/** This Exception used for when there is a problem with odata data stream
  *
  * @param errMsg
  */
class ODataDataException(errMsg: String) extends Exception {}

/** This Exception used for when there is a problem with odata data filter expression
  *
  * @param errMsg
  */
class ODataFilterException(errMsg:String) extends Exception {}


/** This Exception used for when there is a mismatch between expected records count vs actual records count
  *
  * @param errMsg
  */
class ODataRecordsMisMatchException(errMsg:String) extends Exception {}