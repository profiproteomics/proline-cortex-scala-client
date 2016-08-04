package fr.proline.cortex.client.service.dps.msq

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toLong
import fr.proline.core.om.model.msq.ExperimentalDesign
import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.IRemoteServiceConfig
import fr.proline.cortex.client.RemoteServiceExchange
import fr.proline.cortex.client.context.RemoteServiceCallerContext

case class QuantifyConfig(
  name: String,
  description: String,
  methodId: Long,
  experimentalDesign: ExperimentalDesign,
  quantitationConfig: AnyRef
) extends IRemoteServiceConfig {
  
  import QuantifyService.PROCESS_METHOD._
  
  def putInNamedParamMap(namedParams: HashMap[String,Any] ): HashMap[String,Any] = {
    
    namedParams.put(NAME_PARAM, name)
    namedParams.put(DESCRIPTION_PARAM, description)
    namedParams.put(METHOD_ID_PARAM, methodId)
    namedParams.put(EXPERIMENTAL_DESIGN_PARAM, experimentalDesign)
    namedParams.put(QUANTITATION_CONFIG_PARAM, quantitationConfig)
    
    namedParams
  }
}

object Quantify extends AbstractRemoteProcessingServiceCaller[Long] with IQuantifyService {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    config: QuantifyConfig
  )(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceExchange[Long] = {
    
    val namedParams = config.putInNamedParamMap( new HashMap[String,Any]() )
    namedParams.put(PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Long = toLong(responseResult)
  
}