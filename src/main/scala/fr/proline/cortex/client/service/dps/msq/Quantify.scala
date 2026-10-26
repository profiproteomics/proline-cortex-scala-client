package fr.proline.cortex.client.service.dps.msq

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toLong
import fr.proline.core.algo.msq.config.IQuantConfig
import fr.proline.core.om.model.msq.ExperimentalDesign
import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

case class QuantifyConfig(
  name: String,
  description: String,
  methodId: Long,
  experimentalDesign: ExperimentalDesign,
  quantitationConfig: AnyRef // classes extending the IQuantConfig model
) extends INamedParamsBuilder {
  
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

trait IQuantifyServiceCaller extends IServiceCaller {
  def apply(
    projectId: Long,
    config: QuantifyConfig
  ): IServiceCall[Long]
}

case class Quantify()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractRemoteProcessingServiceCaller[Long]
  with IQuantifyService
  with IQuantifyServiceCaller {
  
  import this.PROCESS_METHOD._
  
  // The version of the Quantify service depends on the Cortex profile (see CortexProfile.quantifyServiceVersion)
  override val serviceVersion: String = callerCtx.profile.quantifyServiceVersion
  override val isDefaultVersion: Boolean = false
  
  def apply(
    projectId: Long,
    config: QuantifyConfig
  ): JMSServiceCall[Long] = {
    
    // The version of the LabelFreeQuantConfig depends on the version of the Quantify service called with the Cortex profile
    config.quantitationConfig match {
      case _: fr.proline.core.algo.msq.config.LabelFreeQuantConfig =>
        require(
          callerCtx.profile.labelFreeQuantConfigVersion == "3.0",
          s"the LabelFreeQuantConfig of proline-core (version 3.0) can't be used with the Cortex profile '${callerCtx.profile.name}' " +
          s"(Quantify service ${callerCtx.profile.quantifyServiceVersion}): " +
          "use fr.proline.cortex.client.compat.v2_1.config.LabelFreeQuantConfig (version 2.0) instead"
        )
      case _: fr.proline.cortex.client.compat.v2_1.config.LabelFreeQuantConfig =>
        require(
          callerCtx.profile.labelFreeQuantConfigVersion == "2.0",
          s"the LabelFreeQuantConfig of the package compat.v2_1 (version 2.0) can't be used with the Cortex profile '${callerCtx.profile.name}' " +
          s"(Quantify service ${callerCtx.profile.quantifyServiceVersion}): use the LabelFreeQuantConfig of proline-core (version 3.0) instead"
        )
      case _ => // other configurations are the same for all the profiles
    }
    
    val namedParams = config.putInNamedParamMap( new HashMap[String,Any]() )
    namedParams.put(PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Long = toLong(responseResult)
  
}