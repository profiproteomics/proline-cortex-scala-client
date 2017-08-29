package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.api.service.dps.msi.ImportedResultFile
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

case class ImportResultFileConfig(
  resultFiles: Seq[ResultFileDescriptorRuleId],
  instrumentConfigId: Long,
  peaklistSoftwareId: Long,
  storeSpectrumMatches: Boolean,
  importerProperties: Option[Map[String, Any]] = None
) extends INamedParamsBuilder {
  
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

trait IImportResultFilesServiceCaller extends IServiceCaller {
  def apply(projectId: Long, config: ImportResultFileConfig): IServiceCall[Array[ImportedResultFile]]
}

case class ImportResultFiles()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractRemoteProcessingServiceCaller[Array[ImportedResultFile]]
  with IImportResultFilesServiceV2_0
  with IImportResultFilesServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(projectId: Long, config: ImportResultFileConfig): JMSServiceCall[Array[ImportedResultFile]] = {
    
    val namedParams = config.putInNamedParamMap( new HashMap[String,Any]() )
    namedParams.put(IMPORT_PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Array[ImportedResultFile] = {
    ProfiJson.getObjectMapper().convertValue(responseResult, classOf[Array[ImportedResultFile]])
  }
  
}