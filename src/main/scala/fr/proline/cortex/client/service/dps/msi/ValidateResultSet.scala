package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

trait IValidateResultSetConfig {
  def useTdCompetition: Option[Boolean]
  def pepMatchFilters: Option[Array[FilterConfig]]
  def pepMatchValidatorConfig: Option[PepMatchValidatorConfig]
  def pepSetScoreType: Option[String] // TODO: move fr.proline.core.algo.msi.scoring.PepSetScoring to OM
  def protSetFilters: Option[Array[FilterConfig]]
  def protSetValidatorConfig: Option[ProtSetValidatorConfig]
  //def propagatePepMatchValidation: Option[Boolean]
  //def propagateProtSetValidation: Option[Boolean]
}

case class ValidateResultSetConfig(
  resultSetId: Long,
  description: String,
  useTdCompetition: Option[Boolean],
  pepMatchFilters: Option[Array[FilterConfig]],
  pepMatchValidatorConfig: Option[PepMatchValidatorConfig],
  pepSetScoreType: Option[String], // TODO: move fr.proline.core.algo.msi.scoring.PepSetScoring to OM
  protSetFilters: Option[Array[FilterConfig]],
  protSetValidatorConfig: Option[ProtSetValidatorConfig],
  propagatePepMatchValidation: Option[Boolean] = None,
  propagateProtSetValidation: Option[Boolean] = None
) extends IValidateResultSetConfig with INamedParamsBuilder {
  
  import ValidateResultSetService.PROCESS_METHOD._
  
  def putInNamedParamMap(namedParams: HashMap[String,Any] ): HashMap[String,Any] = {
    
    namedParams.put(RESULT_SET_ID_PARAM, resultSetId)
    namedParams.put(DESCRIPTION_PARAM, description)
    if (useTdCompetition.isDefined) namedParams.put(USE_TD_COMPETITION_PARAM, useTdCompetition.get)
    if (pepMatchFilters.isDefined) namedParams.put(PEP_MATCH_FILTERS_PARAM, pepMatchFilters.get)
    if (pepMatchValidatorConfig.isDefined) namedParams.put(PEP_MATCH_VALIDATOR_CONFIG_PARAM, pepMatchValidatorConfig.get)
    if (pepSetScoreType.isDefined) namedParams.put(PEP_SET_SCORE_TYPE_PARAM, pepSetScoreType.get)
    if (protSetFilters.isDefined) namedParams.put(PROT_SET_FILTERS_PARAM, protSetFilters.get)
    if (protSetValidatorConfig.isDefined) namedParams.put(PROT_SET_VALIDATOR_CONFIG_PARAM, protSetValidatorConfig.get)
    if (propagatePepMatchValidation.isDefined) namedParams.put(PROPAGATE_PEP_MATCH_VALIDATION_PARAM, propagatePepMatchValidation.get)
    if (propagateProtSetValidation.isDefined) namedParams.put(PROPAGATE_PROT_SET_VALIDATION_PARAM, propagateProtSetValidation.get)
    
    namedParams
  }
}

trait IValidateResultSetServiceCaller extends IServiceCaller {
  def apply(projectId: Long, config: ValidateResultSetConfig): IServiceCall[Map[Long,Long]]
}

case class ValidateResultSet()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractRemoteProcessingServiceCaller[Map[Long,Long]]
  with IValidateResultSetServiceV2
  with IValidateResultSetServiceCaller {
  
  import this.PROCESS_METHOD._
  
  // Returns an HashMap corresponding to targetRsmIdByRsId
  def apply(projectId: Long, config: ValidateResultSetConfig): JMSServiceCall[Map[Long,Long]] = {
    
    val namedParams = config.putInNamedParamMap( new HashMap[String,Any]() )
    namedParams.put(PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Map[Long, Long] = {
    ProfiJson.getObjectMapper().convertValue(responseResult, classOf[Map[java.lang.String, Long]]).map { kv =>
      (kv._1.toLong, kv._2)
    }
  }
  
}