package fr.proline.cortex.client.service.dps.msq

import scala.collection.mutable.HashMap

import fr.profi.util.serialization.ProfiJson
import fr.proline.core.om.model.msq.SimplifiedExperimentalDesign
import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

case class QuantifySCConfig(
  name: String,
  description: String,
  experimentalDesign: SimplifiedExperimentalDesign,
  refRsmId: Long,
  refDsId: Long,
  peptideRefRsmIds: Option[Seq[Long]]
) extends INamedParamsBuilder {
  
  import QuantifySCService.PROCESS_METHOD._
  
  def putInNamedParamMap(namedParams: HashMap[String,Any] ): HashMap[String,Any] = {
    
    namedParams.put(NAME_PARAM, name)
    namedParams.put(DESCRIPTION_PARAM, description)
    namedParams.put(EXPERIMENTAL_DESIGN_PARAM, experimentalDesign)
    namedParams.put(REF_RSM_ID_PARAM, refRsmId)
    namedParams.put(REF_DS_ID_PARAM, refDsId)
    if (peptideRefRsmIds.isDefined) namedParams.put(PEPTIDE_REF_RSM_IDS_PARAM, peptideRefRsmIds.get)
    
    namedParams
  }
}

trait IQuantifySCServiceCaller extends IServiceCaller {
  def apply(
    projectId: Long,
    config: QuantifySCConfig
  ): IServiceCall[Map[String,Any]]
}

case class QuantifySC()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractRemoteProcessingServiceCaller[Map[String,Any]]
  with IQuantifySCService
  with IQuantifySCServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    config: QuantifySCConfig
  ): JMSServiceCall[Map[String,Any]] = {
    
    val namedParams = config.putInNamedParamMap( new HashMap[String,Any]() )
    namedParams.put(PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Map[String,Any] = {
    ProfiJson.getObjectMapper().convertValue(responseResult, classOf[Map[String,Any]])
  }
  
}