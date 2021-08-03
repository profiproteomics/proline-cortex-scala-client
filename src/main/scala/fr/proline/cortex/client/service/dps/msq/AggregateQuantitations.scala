package fr.proline.cortex.client.service.dps.msq

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toLong
import fr.proline.core.algo.msq.config.IQuantConfig
import fr.proline.core.om.model.msq.ExperimentalDesign
import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

case class AggregateQuantitationsConfig(
  name: String,
  description: String,
  experimentalDesign: ExperimentalDesign,
  aggregationConfig: fr.proline.core.algo.msq.config.AggregationQuantConfig
) extends INamedParamsBuilder {
  
  import AggregateQuantitationService.PROCESS_METHOD._
  
  def putInNamedParamMap(namedParams: HashMap[String,Any]): HashMap[String,Any] = {
    
    namedParams.put(NAME_PARAM, name)
    namedParams.put(DESCRIPTION_PARAM, description)
    namedParams.put(EXPERIMENTAL_DESIGN_PARAM, experimentalDesign)
    namedParams.put(QUANTITATION_CONFIG_PARAM, aggregationConfig)
    
    namedParams
  }
}

trait IAggregateQuantitationsServiceCaller extends IServiceCaller {
  def apply(
    projectId: Long,
    config: AggregateQuantitationsConfig
  ): IServiceCall[Long]
}

case class AggregateQuantitations()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractRemoteProcessingServiceCaller[Long]
  with IAggregateQuantitationService
  with IAggregateQuantitationsServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    config: AggregateQuantitationsConfig
  ): JMSServiceCall[Long] = {
    
    val namedParams = config.putInNamedParamMap( new HashMap[String,Any]() )
    namedParams.put(PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Long = toLong(responseResult)
  
}