package fr.proline.cortex.client.service.dps.uds

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.api.service.dps.uds._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._
import fr.proline.cortex.client.service.dps.msi.IValidateResultSetConfig

case class ValidateIdentDSTreeConfig(
  parentDatasetIds: Array[Long],
  mergeResultSets: Boolean,
  useTdCompetition: Option[Boolean],
  pepMatchFilters: Option[Array[FilterConfig]],
  pepMatchValidatorConfig: Option[PepMatchValidatorConfig],
  pepSetScoreType: Option[String], // TODO: move fr.proline.core.algo.msi.scoring.PepSetScoring to OM
  protSetFilters: Option[Array[FilterConfig]],
  protSetValidatorConfig: Option[ProtSetValidatorConfig]
) extends IValidateResultSetConfig with INamedParamsBuilder {
  
  import ValidateIdentDSInTreeService.PROCESS_METHOD._
  import ValidateResultSetService.PROCESS_METHOD._
  
  def putInNamedParamMap(namedParams: HashMap[String,Any] ): HashMap[String,Any] = {
    
    namedParams.put(PARENT_DATASET_IDS_PARAM, parentDatasetIds)
    namedParams.put(MERGE_RESULT_SETS_PARAM, mergeResultSets)
    if (useTdCompetition.isDefined) namedParams.put(USE_TD_COMPETITION_PARAM, useTdCompetition.get)
    if (pepMatchFilters.isDefined) namedParams.put(PEP_MATCH_FILTERS_PARAM, pepMatchFilters.get)
    if (pepMatchValidatorConfig.isDefined) namedParams.put(PEP_MATCH_VALIDATOR_CONFIG_PARAM, pepMatchValidatorConfig.get)
    if (pepSetScoreType.isDefined) namedParams.put(PEP_SET_SCORE_TYPE_PARAM, pepSetScoreType.get)
    if (protSetFilters.isDefined) namedParams.put(PROT_SET_FILTERS_PARAM, protSetFilters.get)
    if (protSetValidatorConfig.isDefined) namedParams.put(PROT_SET_VALIDATOR_CONFIG_PARAM, protSetValidatorConfig.get)
    
    namedParams
  }
}

trait IValidateIdentDSTreeServiceCaller extends IServiceCaller {
  def apply(projectId: Long, config: ValidateIdentDSTreeConfig): IServiceCall[Boolean]
}

case class ValidateIdentDSTree()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractRemoteProcessingServiceCaller[Boolean]
  with IValidateIdentDSInTreeService
  with IValidateIdentDSTreeServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(projectId: Long, config: ValidateIdentDSTreeConfig): JMSServiceCall[Boolean] = {
    
    val namedParams = config.putInNamedParamMap( new HashMap[String,Any]() )
    namedParams.put(PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}