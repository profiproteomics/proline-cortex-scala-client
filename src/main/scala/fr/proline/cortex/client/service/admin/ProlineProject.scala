package fr.proline.cortex.client.service.admin

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toLong
import fr.proline.cortex.api.service.admin.ICreateProjectService
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.RemoteServiceCall
import fr.proline.cortex.client.context.RemoteServiceCallerContext

object ProlineProject extends AbstractRemoteProcessingServiceCaller[Long] with ICreateProjectService {
  
  import this.PROCESS_METHOD._
  
  def create(name: String, description: Option[String], ownerId: Long)(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceCall[Long] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(NAME_PARAM, name)
    namedParams.put(OWNER_ID_PARAM, ownerId )
    if (description.isDefined) namedParams.put(DESCRIPTION_PARAM, description.get)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Long = toLong(responseResult)
  
}