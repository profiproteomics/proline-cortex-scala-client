package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.RemoteServiceExchange
import fr.proline.cortex.client.context.RemoteServiceCallerContext

object ChangeRepresentativeProteinMatch extends AbstractRemoteProcessingServiceCaller[Boolean] with IChangeRepresentativeProteinMatchService {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    resultSummaryId: Long,
    rules: Array[RepresentativeProteinMatchRule]
  )(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceExchange[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SUMMARY_ID_PARAM, resultSummaryId )
    namedParams.put(CHANGE_TYPICAL_RULES_PARAM, rules)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}