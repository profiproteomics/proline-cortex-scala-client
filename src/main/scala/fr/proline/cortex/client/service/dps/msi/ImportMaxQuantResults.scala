package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.RemoteServiceExchange
import fr.proline.cortex.client.context.RemoteServiceCallerContext

object ImportMaxQuantResults extends AbstractRemoteProcessingServiceCaller[Map[String, Any]] with IImportMaxQuantResultsService {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    resultFilesDir: String,
    instrumentConfigId: Long,
    peaklistSoftwareId: Long
  )(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceExchange[Map[String, Any]] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_FILES_DIR_PARAM, resultFilesDir)
    namedParams.put(INSTRUMENT_CONFIG_ID_PARAM, instrumentConfigId)
    namedParams.put(PEAKLIST_SOFTWARE_ID_PARAM, peaklistSoftwareId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Map[String, Any] = {
    ProfiJson.getObjectMapper().convertValue(responseResult, classOf[Map[String, Any]])
  }
  
}