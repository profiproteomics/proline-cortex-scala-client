package fr.proline.cortex.client.service.dps.msq

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.core.algo.msq.config.profilizer.PostProcessingConfig
import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

/*
 * Calls the ComputeQuantPostProcessing service (version 2.0), which is available in the Cortex servers of ProlineSuite 2.1 and later.
 * The ComputeQuantProfiles service (config: ProfilizerConfig) is only available in a Cortex 2.1:
 * see fr.proline.cortex.client.compat.v2_1.service.dps.msq.PostProcessQuantitation
 */
trait IPostProcessQuantitationServiceCaller extends IServiceCaller {
  def apply(
    projectId: Long,
    masterQuantChannelId: Long,
    config: PostProcessingConfig
  ): IServiceCall[Boolean]
}

case class PostProcessQuantitation()(implicit val callerCtx: JMSServiceCallerContext) extends AbstractRemoteProcessingServiceCaller[Boolean]
  with IComputeQuantPostProcessingServiceV2
  with IPostProcessQuantitationServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    masterQuantChannelId: Long,
    config: PostProcessingConfig
  ): JMSServiceCall[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(MASTER_QUANT_CHANNEL_ID_PARAM, masterQuantChannelId )
    namedParams.put(CONFIG_PARAM, config )
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}
