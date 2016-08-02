package fr.proline.cortex.client.service.dps.msq

import fr.profi.util.primitives.toLong
import fr.proline.core.om.model.msq.ExperimentalDesign
import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.IRemoteServiceConfig
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

case class QuantifyConfig(
  name: String,
  description: String,
  methodId: java.lang.Long,
  experimentalDesign: ExperimentalDesign,
  quantitationConfig: AnyRef
) extends IRemoteServiceConfig {
  
  import QuantifyService.PROCESS_METHOD._
  
  def putInNamedParamMap(namedParams: java.util.HashMap[String,AnyRef] ): java.util.HashMap[String,AnyRef] = {
    
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
    projectId: java.lang.Long,
    config: QuantifyConfig
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[Long] = {
    
    val namedParams = config.putInNamedParamMap( new java.util.HashMap[String,AnyRef]() )
    namedParams.put(PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Long = toLong(responseResult)
  
}