package fr.proline.cortex.client.service.dps.msq

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.RemoteServiceExchange
import fr.proline.cortex.client.context.RemoteServiceCallerContext

object PostProcessQuantitation extends AbstractRemoteProcessingServiceCaller[Boolean] with IComputeQuantProfilesService {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    masterQuantChannelId: Long,
    // TODO: put the ProfilizerConfig in Proline-OM ?
    config: AnyRef // ProfilizerConfig
  )(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceExchange[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(MASTER_QUANT_CHANNEL_ID_PARAM, masterQuantChannelId )
    namedParams.put(CONFIG_PARAM, config )
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}