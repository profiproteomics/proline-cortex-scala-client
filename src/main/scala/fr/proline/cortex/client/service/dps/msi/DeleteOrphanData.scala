package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

object DeleteOrphanData extends AbstractRemoteProcessingServiceCaller[Boolean] with IDeleteOrphanDataService {
  
  import this.PROCESS_METHOD._
  
  def deleteResultSets(projectId: Long, resultSetIds: Seq[Long])(implicit callerCtx: RemoteServiceCallerContext): Observable[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SET_IDS_PARAM, resultSetIds )
    
    this.runService( Some(namedParams) )
  }
  
  def deleteResultSummaries(projectId: Long, resultSummaryIds: Seq[Long])(implicit callerCtx: RemoteServiceCallerContext): Observable[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SUMMARY_IDS_PARAM, resultSummaryIds )
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}