package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._

trait ICertifyResultFilesServiceCaller {
  def apply(projectId: Long, resultFiles: Seq[ResultFileDescriptorRuleId], importerProperties: Option[Map[String, Any]])(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean]
}

object CertifyResultFiles extends AbstractRemoteProcessingServiceCaller[Boolean]
    with ICertifyResultFilesService
    with ICertifyResultFilesServiceCaller {
  
  import this.PROCESS_METHOD._
  
  // FIXME: return a boolean instead of a string in cortex
  def apply(projectId: Long, resultFiles: Seq[ResultFileDescriptorRuleId], importerProperties: Option[Map[String, Any]])(implicit callerCtx: IServiceCallerContext): JMSServiceCall[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_FILES_PARAM, resultFiles )
    if (importerProperties.isDefined) namedParams.put(IMPORTER_PROPERTIES_PARAM, importerProperties.get)
    
    this.runService( Some(namedParams) )
  }
  
  //protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  protected def convertResponseResult(responseResult: AnyRef): Boolean = {
    responseResult.toString == "OK"
  }
  
}