package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.RemoteServiceCall
import fr.proline.cortex.client.context.RemoteServiceCallerContext

object CertifyResultFiles extends AbstractRemoteProcessingServiceCaller[Boolean] with ICertifyResultFilesService {
  
  import this.PROCESS_METHOD._
  
  def apply(projectId: Long, resultFiles: Seq[ResultFileDescriptorRuleId], importerProperties: Option[Map[String, Any]])(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceCall[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(PROJECT_ID_PARAM, projectId)
    namedParams.put(RESULT_FILES_PARAM, resultFiles )
    if (importerProperties.isDefined) namedParams.put(IMPORTER_PROPERTIES_PARAM, importerProperties.get)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}