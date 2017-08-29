package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

trait IDeleteOrphanDataServiceCaller extends IServiceCaller {
  def deleteResultSets(projectId: Long, resultSetIds: Seq[Long]): IServiceCall[Boolean]
  def deleteResultSummaries(projectId: Long, resultSummaryIds: Seq[Long]): IServiceCall[Boolean]
}

case class DeleteOrphanData()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractRemoteProcessingServiceCaller[Boolean]
  with IDeleteOrphanDataService
  with IDeleteOrphanDataServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def deleteResultSets(projectId: Long, resultSetIds: Seq[Long]): JMSServiceCall[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SET_IDS_PARAM, resultSetIds )
    
    this.runService( Some(namedParams) )
  }
  
  def deleteResultSummaries(projectId: Long, resultSummaryIds: Seq[Long]): JMSServiceCall[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SUMMARY_IDS_PARAM, resultSummaryIds )
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}