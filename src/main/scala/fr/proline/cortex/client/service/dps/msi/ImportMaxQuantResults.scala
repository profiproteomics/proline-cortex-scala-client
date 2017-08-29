package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

trait IImportMaxQuantResultsServiceCaller extends IServiceCaller {
  def apply(
    projectId: Long,
    resultFilesDir: String,
    instrumentConfigId: Long,
    peaklistSoftwareId: Long
  ): IServiceCall[collection.Map[String, Any]]
}

case class ImportMaxQuantResults()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractRemoteProcessingServiceCaller[collection.Map[String, Any]]
  with IImportMaxQuantResultsService
  with IImportMaxQuantResultsServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    resultFilesDir: String,
    instrumentConfigId: Long,
    peaklistSoftwareId: Long
  ): JMSServiceCall[collection.Map[String, Any]] = {
    
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