package fr.proline.cortex.client.service.dps.uds

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toLong
import fr.proline.cortex.api.service.dps.uds._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._

trait IRegisterRawFileServiceCaller {
  def apply(
    rawFileIdentifier: String,
    rawFilePath: Option[String],
    mzdbFilePath: Option[String],
    instrumentId: Long,
    ownerId: Long,
    overwrite: Boolean = false
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Long]
}

object RegisterRawFile extends AbstractRemoteProcessingServiceCaller[Long]
    with IRegisterRawFileService
    with IRegisterRawFileServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(
    rawFileIdentifier: String,
    rawFilePath: Option[String],
    mzdbFilePath: Option[String],
    instrumentId: Long,
    ownerId: Long,
    overwrite: Boolean = false
  )(implicit callerCtx: IServiceCallerContext): JMSServiceCall[Long] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(RAW_FILE_IDENTIFIER_PARAM, rawFileIdentifier)
    if (rawFilePath.isDefined) namedParams.put(RAW_FILE_PATH_PARAM, rawFilePath.get)
    if (mzdbFilePath.isDefined) namedParams.put(MZDB_FILE_PATH_PARAM, mzdbFilePath.get)
    namedParams.put(INSTRUMENT_ID_PARAM, instrumentId)
    namedParams.put(OWNER_ID_PARAM, ownerId)
    namedParams.put(OVERWRITE, overwrite)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Long = toLong(responseResult)
  
}