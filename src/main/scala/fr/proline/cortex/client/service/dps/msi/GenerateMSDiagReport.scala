package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

trait IGenerateMSDiagReportServiceCaller extends IServiceCaller {
  def apply(projectId: Long, resultSetId: Long, msdiagSettings: Map[String, Any]): IServiceCall[String]
}

case class GenerateMSDiagReport()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractRemoteProcessingServiceCaller[String]
  with IGenerateMSDiagReportService
  with IGenerateMSDiagReportServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def apply(projectId: Long, resultSetId: Long, msdiagSettings: Map[String, Any]): JMSServiceCall[String] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SET_ID_PARAM, resultSetId )
    
    // TODO: create a case class to define the MSDIAG settings
    val msdiagSettingsAsJavaMap = ProfiJson.getObjectMapper().convertValue(msdiagSettings, classOf[java.util.Map[String,Object]])
    namedParams.put(MSDIAG_SETTINGS_PARAM, msdiagSettingsAsJavaMap)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): String = responseResult.asInstanceOf[String]
  
}