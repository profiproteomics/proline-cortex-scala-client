package fr.proline.cortex.client.service.dps.msi

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

object ImportMaxQuantResults extends AbstractRemoteProcessingServiceCaller[Map[String, Any]] with IImportMaxQuantResultsService {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: java.lang.Long,
    resultFilesDir: String,
    instrumentConfigId: java.lang.Long,
    peaklistSoftwareId: java.lang.Long
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[Map[String, Any]] = {
    
    val namedParams = new java.util.HashMap[String,AnyRef]()
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