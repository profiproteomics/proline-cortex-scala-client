package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toLong
import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractJSONRPC2ServiceCaller
import fr.proline.cortex.client.RemoteServiceExchange
import fr.proline.cortex.client.context.RemoteServiceCallerContext

object MergeDatasets extends AbstractJSONRPC2ServiceCaller with IMergeResultSetsServiceV2_0 {
  
  def mergeResultSets(
    projectId: Long,
    resultSetIds: Seq[Long],
    additionMode: Option[Boolean]
  )(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceExchange[Long] = {
    
    import MERGE_RESULT_SETS_METHOD._
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SET_IDS_PARAM, resultSetIds )
    if (additionMode.isDefined) namedParams.put(AGGREGATION_MODE_PARAM, additionMode.get)
    
    this.executeRemoteMethod(MERGE_RESULT_SETS_METHOD.name, Some(namedParams), None, toLong )
  }
  
  def mergeResultSummaries(
    projectId: Long,
    resultSummaryIds: Seq[Long],
    additionMode: Option[Boolean]
  )(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceExchange[RSMMergeResult] = {
    
    import MERGE_RESULT_SUMMARIES_METHOD._
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SUMMARY_IDS_PARAM, resultSummaryIds )
    if (additionMode.isDefined) namedParams.put(AGGREGATION_MODE_PARAM, additionMode.get)
    
    this.executeRemoteMethod(MERGE_RESULT_SUMMARIES_METHOD.name, Some(namedParams), None, { responseResult =>
      ProfiJson.getObjectMapper().convertValue(responseResult, classOf[RSMMergeResult])
    })
  }
  
}