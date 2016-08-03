package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toInt
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

object UpdateSpectraParams extends AbstractRemoteProcessingServiceCaller[Int] with IUpdateSpectraParamsServiceV2_0 {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    resultSetIds: Seq[Long],
    peaklistSoftwareId: Long
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[Int] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SET_IDS_PARAM, resultSetIds )
    namedParams.put(PEAKLIST_SOFTWARE_ID_PARAM, peaklistSoftwareId )
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Int = toInt(responseResult)
  
}