package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toLong
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

object ImportValidateGenerateSM extends AbstractRemoteProcessingServiceCaller[Long] with IImportValidateGenerateSMService {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    
    // Import specific parameters
    importResultFileConfig: ImportResultFileConfig,
    
    // Validation specific parameters
    validateResultSetConfig: ValidateResultSetConfig,
    
    // "Generate Spectrum Matches" specific parameters
    generateSpectrumMatches: Option[Boolean] = None,
    forceInsert: Option[Boolean] = None
    
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[Long] = {
    
    val namedParams = new HashMap[String,Any]()
    
    // Put import specific parameters in Named Parameters Map
    importResultFileConfig.putInNamedParamMap( namedParams )
    
    // Put validation specific parameters in Named Parameters Map
    validateResultSetConfig.putInNamedParamMap( namedParams )
    
    // Put "Generate Spectrum Matches" specific parameters in Named Parameters Map
    if (generateSpectrumMatches.isDefined) namedParams.put(GENERATE_SPECTRUM_MATCHES_PARAM, generateSpectrumMatches.get)
    if (forceInsert.isDefined) namedParams.put(FORCE_INSERT_PARAM, forceInsert.get)

    this.runService( Some(namedParams) )
  }
  
  /*
  def apply(
    projectId: Long,
    
    // Import specific parameters
    resultFiles: Seq[ResultFileDescriptorRuleId],
    instrumentConfigId: Long,
    peaklistSoftwareId: Long,
    importerProperties: Option[Map[String, Any]] = None,
    
    // Validation specific parameters
    pepMatchFilters: Option[Array[FilterConfig]],
    pepMatchValidatorConfig: Option[PepMatchValidatorConfig],
    pepSetScoreType: Option[String], // TODO: move fr.proline.core.algo.msi.scoring.PepSetScoring to OM
    protSetFilters: Option[Array[FilterConfig]],
    protSetValidatorConfig: Option[ProtSetValidatorConfig],
    
    // Generate Spectrum Matches specific parameters
    generateSpectrumMatches: Option[Boolean],
    forceInsert: Option[Boolean] = None
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[Long] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(IMPORT_PROJECT_ID_PARAM, projectId)
    
    namedParams.put(USE_DECOY_REGEX_PARAM, false: Boolean)
    namedParams.put(RESULT_FILES_PARAM, resultFiles)
    namedParams.put(INSTRUMENT_CONFIG_ID_PARAM, instrumentConfigId)
    namedParams.put(PEAKLIST_SOFTWARE_ID_PARAM, peaklistSoftwareId)
    namedParams.put(SAVE_SPECTRUM_MATCHES_PARAM, false: Boolean)
    if (importerProperties.isDefined) namedParams.put(IMPORTER_PROPERTIES_PARAM, importerProperties.get)
    
    if (pepMatchFilters.isDefined) namedParams.put(PEP_MATCH_FILTERS_PARAM, pepMatchFilters.get)
    if (pepMatchValidatorConfig.isDefined) namedParams.put(PEP_MATCH_VALIDATOR_CONFIG_PARAM, pepMatchValidatorConfig.get)
    if (pepSetScoreType.isDefined) namedParams.put(PEP_SET_SCORE_TYPE_PARAM, pepSetScoreType.get)
    if (protSetFilters.isDefined) namedParams.put(PROT_SET_FILTERS_PARAM, protSetFilters.get)
    if (protSetValidatorConfig.isDefined) namedParams.put(PROT_SET_VALIDATOR_CONFIG_PARAM, protSetValidatorConfig.get)
    
    if (generateSpectrumMatches.isDefined) namedParams.put(GENERATE_SPECTRUM_MATCHES_PARAM, generateSpectrumMatches.get)
    if (forceInsert.isDefined) namedParams.put(FORCE_INSERT_PARAM, forceInsert.get)

    this.runService( Some(namedParams) )
  }*/
  
  protected def convertResponseResult(responseResult: AnyRef): Long = toLong(responseResult)
  
}