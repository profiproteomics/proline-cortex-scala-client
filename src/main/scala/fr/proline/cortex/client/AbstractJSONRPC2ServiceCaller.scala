package fr.proline.cortex.client

import java.util.concurrent.atomic.AtomicLong

import javax.jms.Message
import javax.jms.TextMessage

import com.thetransactioncompany.jsonrpc2.JSONRPC2Request
import com.thetransactioncompany.jsonrpc2.JSONRPC2Response

import fr.proline.cortex.client.context.RemoteServiceCallerContext
import fr.proline.jms.util.JMSConstants

import rx.lang.scala.Observable

private[this] object AbstractJSONRPC2ServiceCaller {
  
  private val jsonRequestIdSequence = new AtomicLong(0)
  def newJsonRequestId(): Long = { jsonRequestIdSequence.incrementAndGet() }
  
}

abstract class AbstractJSONRPC2ServiceCaller[T] extends AbstractRemoteServiceCaller[AnyRef] {
  
  protected val MAX_LOGGING_RESPONSE_LENGTH = 1000
  
  /**
   * Abstract method used to convert the remote service response result.
   * @param responseResult The AnyRef result which will be converted to the generic type T.
   */
  protected def convertResponseResult(responseResult: AnyRef): T
  
  /**
   * Method used to parse the remote service response.
   * @param jmsMessage The JMS message to parse.
   */
  protected def parseRemoteServiceResponse(jmsMessage: Message): AnyRef = {
    
    val jmsMessageID = jmsMessage.getJMSMessageID
    
    var responseLength = 0
    val jsonResponse = jmsMessage match {
      
      case textMessage: TextMessage => {
        val responseText = textMessage.getText()
        responseLength = responseText.length
        
        if (responseLength == 0) {
          throw newServiceException(
            s"invalid JMS Message ($jmsMessageID), the string must not be empty to be parsed a JSON-RPC response !"
          )
        }
        
        JSONRPC2Response.parse(responseText)
      }
      case _ => throw newServiceException(
        "invalid JMS Message($jmsMessageID), it must be a TextMessage to be converted to a JSONRPC2Response !"
      )
    }
    
    logger.debug(s"JSON response ID from service '$serviceName' = " + jsonResponse.getID())

    val jsonError = jsonResponse.getError()
    
    if (jsonError != null) {
      logger.error(
        s"Error while executing '$serviceName' service: " +
        s"JSON Error code ${jsonError.getCode()}, message: ${jsonError.getMessage()}"
      )
      
      throw jsonError
    } else {
      if (responseLength <= MAX_LOGGING_RESPONSE_LENGTH) {
        logger.debug(s"JSON response result from service '$serviceName': " + jsonResponse.getResult())
      }
    }
    
    jsonResponse.getResult
  }
  
  /**
   * Execute the remote JSON-RPC method and wait for the response.
   * @param methodName The name of the method to execute remotely.
   * @param namedParameters A Map of named parameters that will be sent in the request.
   */
  def executeRemoteMethod(
    methodName: String,
    namedParams: Option[java.util.Map[String,AnyRef]] = None
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[T] = {
    
    val jsonRpcRequest = new JSONRPC2Request(methodName, AbstractJSONRPC2ServiceCaller.newJsonRequestId())
    
    if (namedParams.isDefined) {
      jsonRpcRequest.setNamedParams(namedParams.get)
    }
    
    val textMessage = callerCtx.jmsContext.createTextMessage(jsonRpcRequest.toJSONString())
    textMessage.setStringProperty(JMSConstants.PROLINE_SERVICE_NAME_KEY, serviceName)
    
    this.callRemoteService(textMessage).map { result: AnyRef =>
      this.convertResponseResult(result)
    }
  }

}
