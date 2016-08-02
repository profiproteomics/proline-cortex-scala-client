package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap
import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.context.RemoteServiceCallerContext
import rx.lang.scala.Observable
import fr.profi.util.serialization.ProfiJson

object GenerateMSDiagReport extends AbstractRemoteProcessingServiceCaller[String] with IGenerateMSDiagReportService {
  
  import this.PROCESS_METHOD._
  
  def apply(projectId: java.lang.Long, resultSetId: java.lang.Long, msdiagSettings: Map[String, Any])(implicit callerCtx: RemoteServiceCallerContext): Observable[String] = {
    
    val namedParams = new java.util.HashMap[String,AnyRef]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_SET_ID_PARAM, resultSetId )
    
    // TODO: create a case class to define the MSDIAG settings
    val msdiagSettingsAsJavaMap = ProfiJson.getObjectMapper().convertValue(msdiagSettings, classOf[java.util.Map[String,Object]])
    namedParams.put(MSDIAG_SETTINGS_PARAM, msdiagSettingsAsJavaMap)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): String = responseResult.asInstanceOf[String]
  
}