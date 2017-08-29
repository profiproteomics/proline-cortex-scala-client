package fr.proline.cortex.client.service.dps.uds

import scala.collection.Map
import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.uds._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

trait IGetExportInformationServiceCaller extends IServiceCaller {
  def apply(projectId: Long, datasetId: Long, extraParams: Option[Map[String, Any]]): IServiceCall[String]
}

case class GetExportInformation()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractRemoteProcessingServiceCaller[String]
  with IGetExportInformationService
  with IGetExportInformationServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(projectId: Long, datasetId: Long, extraParams: Option[Map[String, Any]]): JMSServiceCall[String] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(DATASET_ID_PARAM, datasetId )
    if (extraParams.isDefined) namedParams.put(EXTRA_PARAMS_PARAM, extraParams.get)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): String = responseResult.asInstanceOf[String]
  
}