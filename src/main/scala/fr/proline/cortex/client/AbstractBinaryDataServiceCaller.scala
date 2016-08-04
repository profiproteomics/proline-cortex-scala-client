package fr.proline.cortex.client

import javax.jms.BytesMessage
import javax.jms.Message
import javax.jms.TextMessage

import scala.collection.mutable.HashMap

import fr.proline.cortex.client.context.BuildRemoteServiceCallerContext
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable
import rx.lang.scala.Subscriber

abstract class AbstractBinaryDataServiceCaller extends AbstractJSONRPC2ServiceCaller {
  
  /**
   * Execute the remote JSON-RPC method and wait for the response.
   * @param methodName The name of the method to execute remotely.
   * @param namedParameters A Map of named parameters that will be sent in the request.
   * @param convertResponseResult A function used to convert the response from AnyRef to generic T type.
   */
  def executeRemoteMethod(
    methodName: String,
    namedParams: Option[HashMap[String,Any]],
    annotateRequest: Option[TextMessage => Unit]
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[BytesMessage] = {
    
    val newJmsContext = callerCtx.jmsContext.createContext(callerCtx.jmsContext.getSessionMode)
    val localCallerContext = BuildRemoteServiceCallerContext()(newJmsContext)
    
    val bytesMessageObservable = super.executeRemoteMethod[BytesMessage](
      methodName,
      namedParams,
      annotateRequest,
      _.asInstanceOf[BytesMessage],
      synchronous = true
    )(localCallerContext)
    
    bytesMessageObservable.doAfterTerminate {
      localCallerContext.close()
    }
  }
  
  /**
   * Method used to parse and emit the remote service response.
   * @param jmsMessage The JMS message to parse.
   */
  override protected def emitRemoteServiceResponse(jmsMessage: Message, responseSuscriber: Subscriber[AnyRef]): Unit = {
    
    val jmsMessageID = jmsMessage.getJMSMessageID
    
    jmsMessage match {
      case bytesMessage: BytesMessage => {
        responseSuscriber.onNext(bytesMessage)
      }
      case textMessage: TextMessage => throw newServiceException(
        s"invalid JMS Message($jmsMessageID), it must be a BytesMessage and not a TextMessage (${textMessage.getText}) !"
      )
      case _ => throw newServiceException(
        s"invalid JMS Message($jmsMessageID), it must be a BytesMessage !"
      )
    }
    
  }
  
}
