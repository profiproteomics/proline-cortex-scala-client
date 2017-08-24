package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._

trait IFilterRSMProteinSetsServiceCaller {
  def apply(
    projectId: Long,
    resultSummaryId: Long,
    protSetFilterConfigs: Seq[FilterConfig]
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean]
}

object FilterRSMProteinSets extends AbstractRemoteProcessingServiceCaller[Boolean]
  with IFilterRSMProteinSetsService
  with IFilterRSMProteinSetsServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(projectId: Long, resultSummaryId: Long, protSetFilterConfigs: Seq[FilterConfig])(implicit callerCtx: IServiceCallerContext): JMSServiceCall[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SUMMARY_ID_PARAM, resultSummaryId )
    namedParams.put(PROT_SET_FILTERS_PARAM, protSetFilterConfigs)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}