package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

object FilterRSMProteinSets extends AbstractRemoteProcessingServiceCaller[Boolean] with IFilterRSMProteinSetsService {
  
  import this.PROCESS_METHOD._
  
  def apply(projectId: java.lang.Long, resultSummaryId: java.lang.Long, protSetFilters: Seq[FilterConfig])(implicit callerCtx: RemoteServiceCallerContext): Observable[Boolean] = {
    
    val namedParams = new java.util.HashMap[String,AnyRef]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SUMMARY_ID_PARAM, resultSummaryId )
    namedParams.put(PROT_SET_FILTERS_PARAM, protSetFilters)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}