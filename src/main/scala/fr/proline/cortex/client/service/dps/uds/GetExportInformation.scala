package fr.proline.cortex.client.service.dps.uds

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.uds._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

object GetExportInformation extends AbstractRemoteProcessingServiceCaller[String] with IGetExportInformationService {
  
  import this.PROCESS_METHOD._
  
  def apply(projectId: java.lang.Long, datasetId: java.lang.Long, extraParams: Option[Map[String, Any]])(implicit callerCtx: RemoteServiceCallerContext): Observable[String] = {
    
    val namedParams = new java.util.HashMap[String,AnyRef]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(DATASET_ID_PARAM, datasetId )
    if (extraParams.isDefined) namedParams.put(EXTRA_PARAMS_PARAM, extraParams.get)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): String = responseResult.asInstanceOf[String]
  
}