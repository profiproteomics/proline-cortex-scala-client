package fr.proline.cortex.client.service.dps.msi

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.api.service.dps.msi.ImportResultFilesServiceV2_0.{ ImportedResultFile => ResultFile }
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.IRemoteServiceConfig
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

case class ImportResultFileConfig(
  resultFiles: Seq[ResultFileDescriptorRuleId],
  instrumentConfigId: java.lang.Long,
  peaklistSoftwareId: java.lang.Long,
  storeSpectrumMatches: java.lang.Boolean,
  importerProperties: Option[Map[String, Any]] = None
) extends IRemoteServiceConfig {
  
  import ImportResultFilesServiceV2_0.PROCESS_METHOD._
  
  def putInNamedParamMap(namedParams: java.util.HashMap[String,AnyRef] ): java.util.HashMap[String,AnyRef] = {
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
  
  def apply(projectId: java.lang.Long, config: ImportResultFileConfig)(implicit callerCtx: RemoteServiceCallerContext): Observable[Array[ResultFile]] = {
    
    val namedParams = config.putInNamedParamMap( new java.util.HashMap[String,AnyRef]() )
    namedParams.put(IMPORT_PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Array[ResultFile] = {
    ProfiJson.getObjectMapper().convertValue(responseResult, classOf[Array[ResultFile]])
  }
  
}