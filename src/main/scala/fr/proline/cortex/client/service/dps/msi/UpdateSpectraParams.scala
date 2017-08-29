package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toInt
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

trait IUpdateSpectraParamsServiceCaller extends IServiceCaller {
  def apply(
    projectId: Long,
    resultSetIds: Seq[Long],
    peaklistSoftwareId: Long
  ): IServiceCall[Int]
}

case class UpdateSpectraParams()(implicit val callerCtx: JMSServiceCallerContext)
    extends AbstractRemoteProcessingServiceCaller[Int]
    with IUpdateSpectraParamsServiceV2_0
    with IUpdateSpectraParamsServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(
    projectId: Long,
    resultSetIds: Seq[Long],
    peaklistSoftwareId: Long
  ): JMSServiceCall[Int] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SET_IDS_PARAM, resultSetIds )
    namedParams.put(PEAKLIST_SOFTWARE_ID_PARAM, peaklistSoftwareId )
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Int = toInt(responseResult)
  
}