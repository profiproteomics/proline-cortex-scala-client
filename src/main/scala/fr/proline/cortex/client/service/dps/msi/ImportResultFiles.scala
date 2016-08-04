package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.api.service.dps.msi.ImportResultFilesServiceV2_0.{ ImportedResultFile => ResultFile }
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.RemoteServiceExchange
import fr.proline.cortex.client.IRemoteServiceConfig
import fr.proline.cortex.client.context.RemoteServiceCallerContext

case class ImportResultFileConfig(
  resultFiles: Seq[ResultFileDescriptorRuleId],
  instrumentConfigId: Long,
  peaklistSoftwareId: Long,
  storeSpectrumMatches: Boolean,
  importerProperties: Option[Map[String, Any]] = None
) extends IRemoteServiceConfig {
  
  import ImportResultFilesServiceV2_0.PROCESS_METHOD._
  
  def putInNamedParamMap(namedParams: HashMap[String,Any] ): HashMap[String,Any] = {
    namedParams.put(RESULT_FILES_PARAM_V2_0, resultFiles)
    namedParams.put(INSTRUMENT_CONFIG_ID_PARAM, instrumentConfigId)
    namedParams.put(PEAKLIST_SOFTWARE_ID_PARAM, peaklistSoftwareId)
    namedParams.put(SAVE_SPECTRUM_MATCHES_PARAM, storeSpectrumMatches)
    if (importerProperties.isDefined) namedParams.put(IMPORTER_PROPERTIES_PARAM, importerProperties.get)
    
    namedParams
  }
}

object ImportResultFiles extends AbstractRemoteProcessingServiceCaller[Array[ResultFile]] with IImportResultFilesServiceV2_0 {
  
  import this.PROCESS_METHOD._
  
  def apply(projectId: Long, config: ImportResultFileConfig)(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceExchange[Array[ResultFile]] = {
    
    val namedParams = config.putInNamedParamMap( new HashMap[String,Any]() )
    namedParams.put(IMPORT_PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Array[ResultFile] = {
    ProfiJson.getObjectMapper().convertValue(responseResult, classOf[Array[ResultFile]])
  }
  
}