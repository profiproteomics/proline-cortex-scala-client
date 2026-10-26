package fr.proline.cortex.client.compat.v2_1.service.dps.msq

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.core.algo.msq.config.profilizer.ProfilizerConfig
import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

/*
 * Calls the ComputeQuantProfiles service, which only exists in a Cortex server of ProlineSuite 2.1 (removed afterwards).
 * Must be used with a CortexProfile.V2_1. With a Cortex server providing the ComputeQuantPostProcessing service,
 * use fr.proline.cortex.client.service.dps.msq.PostProcessQuantitation instead.
 */
trait IPostProcessQuantitationServiceCaller extends IServiceCaller {
  def apply(
    projectId: Long,
    masterQuantChannelId: Long,
    config: ProfilizerConfig
  ): IServiceCall[Boolean]
}

case class PostProcessQuantitation()(implicit val callerCtx: JMSServiceCallerContext) extends AbstractRemoteProcessingServiceCaller[Boolean]
  with IComputeQuantProfilesService
  with IPostProcessQuantitationServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    masterQuantChannelId: Long,
    config: ProfilizerConfig
  ): JMSServiceCall[Boolean] = {
    
    require(
      callerCtx.profile.supportsComputeQuantProfilesService,
      s"the ComputeQuantProfiles service is not available with the Cortex profile '${callerCtx.profile.name}': use PostProcessQuantitation from package fr.proline.cortex.client.service.dps.msq"
    )
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(MASTER_QUANT_CHANNEL_ID_PARAM, masterQuantChannelId )
    namedParams.put(CONFIG_PARAM, config )
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}