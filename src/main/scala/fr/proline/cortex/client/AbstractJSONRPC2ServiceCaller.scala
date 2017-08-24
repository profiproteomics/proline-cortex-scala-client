package fr.proline.cortex.client

import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern

import javax.jms.Message
import javax.jms.TextMessage

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.thetransactioncompany.jsonrpc2.JSONRPC2Response

import fr.profi.util.concurrent.ObservableUtils
import fr.profi.util.exception.SerializableStackTraceElement
import fr.profi.util.jsonrpc.SerializableJSONRPC2Request
import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.caller._
import fr.proline.cortex.client.context._

import rx.lang.scala.Observable
import rx.lang.scala.Subscriber

private[this] object AbstractJSONRPC2ServiceCaller {
  
  private val jsonRequestIdSequence = new AtomicLong(0)
  def newJsonRequestId(): Long = { jsonRequestIdSequence.incrementAndGet() }
  
}

abstract class AbstractJSONRPC2ServiceCaller extends AbstractRemoteServiceCaller[AnyRef] {
  
  protected val MAX_LOGGING_RESPONSE_LENGTH = 1000
  
  /**
   * Method used to parse and emit the remote service response.
   * @param jmsMessage The JMS message to parse.
   */
  protected def emitRemoteServiceResponse(jmsMessage: Message, responseSuscriber: Subscriber[AnyRef]): Unit = {
    
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
        
        // TODO: use ProfiJSONRPC2Response ???
        JSONRPC2Response.parse(responseText)
      }
      case _ => throw newServiceException(
        s"invalid JMS Message($jmsMessageID), it must be a TextMessage to be converted to a JSONRPC2Response !"
      )
    }
    
    logger.debug(s"JSON response ID from service '$serviceName' = " + jsonResponse.getID())

    val jsonError = jsonResponse.getError()
    
    if (jsonError != null) {
      
      val errData = jsonError.getData
      if (errData != null) {
        try {
          val jsonData = jsonError.getData()
          val stackTrace = ProfiJson.getObjectMapper().convertValue(jsonData, classOf[Array[SerializableStackTraceElement]])
          
          jsonError.setStackTrace(stackTrace.map(_.toStackTraceElement()))
        } catch {
          case t: Throwable => {
            logger.error("Can't set the stack trace of the JSONRPC2Error", t)
            
            logger.debug("JSON response was:\n" +jmsMessage.asInstanceOf[TextMessage].getText)
          }
        }
      }
      
      throw jsonError
      
    } else {
      if (responseLength <= MAX_LOGGING_RESPONSE_LENGTH) {
        logger.debug(s"JSON response result from service '$serviceName': " + jsonResponse.getResult())
      }
    }
    
    // Emit the response
    responseSuscriber.onNext(jsonResponse.getResult)
  }
  
  /**
   * Execute the remote JSON-RPC method and wait for the response.
   * @param methodName The name of the method to execute remotely.
   * @param namedParameters A Map of named parameters that will be sent in the request.
   * @param convertResponseResult A function used to convert the response from AnyRef to generic T type.
   */
  def executeRemoteMethod[T](
    methodName: String,
    namedParams: Option[HashMap[String,Any]],
    annotateRequest: Option[TextMessage => Unit],
    convertResponseResult: AnyRef => T,
    synchronous: Boolean = false
  )(implicit callerCtx: IServiceCallerContext): JMSServiceCall[T] = {
    
    val (futureRequest, observableConvertedResult) = this.observeRemoteMethodResult(
      methodName,
      namedParams,
      annotateRequest,
      convertResponseResult,
      synchronous
    )
    
    new JMSServiceCall(futureRequest, ObservableUtils.observableToFuture(observableConvertedResult))
  }
  
  /**
   * Execute the remote JSON-RPC method and wait for the response.
   * @param methodName The name of the method to execute remotely.
   * @param namedParameters A Map of named parameters that will be sent in the request.
   * @param convertResponseResult A function used to convert the response from AnyRef to generic T type.
   */
  protected def observeRemoteMethodResult[T](
    methodName: String,
    namedParams: Option[HashMap[String,Any]],
    annotateRequest: Option[TextMessage => Unit],
    convertResponseResult: AnyRef => T,
    synchronous: Boolean
  )(implicit callerCtx: IServiceCallerContext): (Future[Message],Observable[T]) = {
    
    implicit val jmsCallerCtx: JMSServiceCallerContext = callerCtx
    
    logger.debug(s"Serializing JSON-RPC request for service '$serviceName' and method '$methodName'")
    
    // Convert the namedParams to JSONRPC2Request compliant java.util.Map
    val jsonRpcRequest = new SerializableJSONRPC2Request(
      id = AbstractJSONRPC2ServiceCaller.newJsonRequestId(),
      method = methodName,
      params = namedParams.orNull
    )
    
    val textMessage = jmsCallerCtx.jmsContext.createTextMessage(jsonRpcRequest.toJSONString)
    if (annotateRequest.isDefined) annotateRequest.get.apply(textMessage)
    
    val (futureRequest, observableResult) = this.callRemoteService(textMessage, synchronous)
    
    val observableConvertedResult = observableResult.map { result: AnyRef =>
      convertResponseResult(result)
    }
    
    (futureRequest, observableConvertedResult)
  }
  
}
