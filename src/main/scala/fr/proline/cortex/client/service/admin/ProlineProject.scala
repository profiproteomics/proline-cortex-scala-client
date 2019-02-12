package fr.proline.cortex.client.service.admin

import javax.jms.Message

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toLong
import fr.proline.cortex.api.service.admin.ICreateProjectService
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

trait IProlineProjectServiceCaller extends IServiceCaller {
  def create(name: String, description: Option[String], ownerId: Long): IServiceCall[Long]
}

case class ProlineProject()(implicit val callerCtx: JMSServiceCallerContext) extends AbstractRemoteProcessingServiceCaller[Long]
    with ICreateProjectService
    with IProlineProjectServiceCaller {
  
  import this.PROCESS_METHOD._
  
  def create(name: String, description: Option[String], ownerId: Long): JMSServiceCall[Long] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(NAME_PARAM, name)
    namedParams.put(OWNER_ID_PARAM, ownerId )
    if (description.isDefined) namedParams.put(DESCRIPTION_PARAM, description.get)
    
    val result = this.runService( Some(namedParams) )
    
    result
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Long = toLong(responseResult)
  
}