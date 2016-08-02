package fr.proline.cortex.client.service.dps.msq

import fr.profi.util.serialization.ProfiJson
import fr.proline.core.om.model.msq.SimplifiedExperimentalDesign
import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.IRemoteServiceConfig
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

case class QuantifySCConfig(
  name: String,
  description: String,
  experimentalDesign: SimplifiedExperimentalDesign,
  refRsmId: java.lang.Long,
  refDsId: java.lang.Long,
  peptideRefRsmIds: Option[Array[Long]]
) extends IRemoteServiceConfig {
  
  import QuantifySCService.PROCESS_METHOD._
  
  def putInNamedParamMap(namedParams: java.util.HashMap[String,AnyRef] ): java.util.HashMap[String,AnyRef] = {
    
    namedParams.put(NAME_PARAM, name)
    namedParams.put(DESCRIPTION_PARAM, description)
    namedParams.put(EXPERIMENTAL_DESIGN_PARAM, experimentalDesign)
    namedParams.put(REF_RSM_ID_PARAM, refRsmId)
    namedParams.put(REF_DS_ID_PARAM, refDsId)
    if (peptideRefRsmIds.isDefined) namedParams.put(PEPTIDE_REF_RSM_IDS_PARAM, peptideRefRsmIds.get)
    
    namedParams
  }
}

object QuantifySC extends AbstractRemoteProcessingServiceCaller[Map[String,Any]] with IQuantifySCService {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: java.lang.Long,
    config: QuantifySCConfig
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[Map[String,Any]] = {
    
    val namedParams = config.putInNamedParamMap( new java.util.HashMap[String,AnyRef]() )
    namedParams.put(PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Map[String,Any] = {
    ProfiJson.getObjectMapper().convertValue(responseResult, classOf[Map[String,Any]])
  }
  
}