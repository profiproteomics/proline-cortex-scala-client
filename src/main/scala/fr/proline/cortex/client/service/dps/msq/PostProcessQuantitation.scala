package fr.proline.cortex.client.service.dps.msq

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.core.algo.msq.config.profilizer.ProfilizerConfig
import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._

trait IPostProcessQuantitationServiceCaller {
  def apply(
    projectId: Long,
    masterQuantChannelId: Long,
    config: ProfilizerConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean]
}

object PostProcessQuantitation extends AbstractRemoteProcessingServiceCaller[Boolean]
  with IComputeQuantProfilesService
  with IPostProcessQuantitationServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    masterQuantChannelId: Long,
    config: ProfilizerConfig
  )(implicit callerCtx: IServiceCallerContext): JMSServiceCall[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(MASTER_QUANT_CHANNEL_ID_PARAM, masterQuantChannelId )
    namedParams.put(CONFIG_PARAM, config )
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}