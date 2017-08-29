package fr.proline.cortex.client.service.misc

import scala.collection.mutable.HashMap

import fr.profi.util.primitives._
import fr.proline.cortex.api.service.misc.ICancelService
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

trait IServiceManagerServiceCaller {
  def cancelService(jmsMessageId: String): IServiceCall[Boolean]
}

// Note: we intentionally renamed CancelService into ServiceManager
// TODO: change the name accordingly in Cortex
case class ServiceManager()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractJSONRPC2ServiceCaller
  with ICancelService
  with IServiceManagerServiceCaller {
  
  def cancelService(jmsMessageId: String): JMSServiceCall[Boolean] = {
    val namedParams = new HashMap[String,Any]()
    namedParams.put(CANCEL_METHOD.MESSAGE_ID_PARAM, jmsMessageId)
    
    this.executeRemoteMethod(CANCEL_METHOD.name, Some(namedParams), None, result => toBoolean(result))
  }
  
}