package fr.proline.cortex.client.service.misc

import scala.collection.mutable.HashMap

import fr.profi.util.primitives._
import fr.proline.cortex.api.service.misc.ICancelService
import fr.proline.cortex.caller._
import fr.proline.cortex.client._

// Note: we intentionally renamed CancelService into ServiceManager
object ServiceManager extends AbstractJSONRPC2ServiceCaller with ICancelService {
  
  def cancelService(jmsMessageId: String)(implicit callerCtx: IServiceCallerContext): JMSServiceCall[Boolean] = {
    val namedParams = new HashMap[String,Any]()
    namedParams.put(CANCEL_METHOD.MESSAGE_ID_PARAM, jmsMessageId)
    
    this.executeRemoteMethod(CANCEL_METHOD.name, Some(namedParams), None, result => toBoolean(result))
  }
  
}