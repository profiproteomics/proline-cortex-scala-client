package fr.proline.cortex.client

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger

import javax.jms._

import scala.concurrent.Future

import com.thetransactioncompany.jsonrpc2._
import com.typesafe.scalalogging.StrictLogging

import fr.proline.cortex.api._
import fr.proline.cortex.api.CortexConstants._
import fr.proline.cortex.client.context.RemoteServiceCallerContext
import fr.proline.jms.service.api.IRemoteServiceIdentity
import fr.proline.jms.util.JMSConstants
import fr.proline.jms.util.JMSUtils

import rx.lang.scala.Observable
import rx.lang.scala.Subscriber

/**
 * Abstract class defining minimal behavior for all remote service callers.
 * 
 * @param callback
 *   The callback which will be executed
 * @param synchronous
 *   Specify the kind of message communication.
 *   If true then the caller will wait for the response.
 *   If false then the response will be obtained asynchronously (using a MessageListener).
 * @param responseTimeout
 *   Specify the timeout value (in milliseconds) to wait for message.
 *   This is only used for synchronous tasks. 
 *   Default value is 10000 ms.
 */
abstract class AbstractRemoteServiceCaller[T] extends IRemoteServiceIdentity with StrictLogging {
  
  /* To count received messages */
  private val messagesCountSequence = new AtomicInteger(0)
  
  /**
   * Abstract method used to parse and emit the remote service response.
   * Important: this method must call the onNext method of the provided Subscriber.
   *
   * @param jmsMessage The JMS message to parse.
   */
  protected def emitRemoteServiceResponse(jmsMessage: Message, suscriber: Subscriber[T]): Unit
  
  /**
   * Call the remote service by sending a JMS request (Message) and waiting for a JMS response (Message).
   * If the JMS response is null then an Exception is thrown.
   * Else the JMS response is parsed using the ''emitRemoteServiceResponse'' method.
   */
  protected def callRemoteService(jmsRequest: Message, synchronous: Boolean)(implicit callerCtx: RemoteServiceCallerContext): (Future[Message],Observable[T]) = {
    
    // Add some header properties to target the appropriate service and its corresponding version 
    jmsRequest.setStringProperty(JMSConstants.PROLINE_SERVICE_NAME_KEY, serviceName)
    jmsRequest.setStringProperty(JMSConstants.PROLINE_SERVICE_VERSION_KEY, serviceVersion)
    this.addServiceClientToMessage(jmsRequest)
    
    val promise = scala.concurrent.Promise[Message]()
    
    val observable = Observable[T] { suscriber =>
      
      val startingTime = System.currentTimeMillis()
      
      val requestType = if (synchronous) "synchronous" else "asynchronous"
      logger.info(s"RemoteServiceCaller will send a $requestType JMS request for service '$serviceName'")
      
      //val latch = new CountDownLatch(1)
      
      // Create a temporary queue for the replies consumption
      val temporaryReplyQueue = callerCtx.jmsContext.createTemporaryQueue()
      
      // Tell the remote service to reply to a particular queue: a TemporaryQueue used to store Server->Client responses
      jmsRequest.setJMSReplyTo(temporaryReplyQueue)
      
      // Create new MessageConsumer that will be used to listen for the remote service response
      val responseConsumer = callerCtx.jmsContext.createConsumer(temporaryReplyQueue)
      
      val messageListener = new Object with MessageListener {
        override def onMessage(jmsResponse: Message) {
          Future {
            val messageNumber = messagesCountSequence.incrementAndGet()
            
            try {
              val endingTime = System.currentTimeMillis()
              val requestDuration = (endingTime - startingTime) / 1000
              logger.debug(s"Received JMS response from remote service '$serviceName' after $requestDuration seconds.")
              
              if (jmsResponse != null) {
                logger.debug(s"Received message #$messageNumber ${jmsResponse.getJMSMessageID} from remote service '$serviceName'")
                logger.trace(s"Content of message ${jmsResponse.getJMSMessageID} is:" + JMSUtils.formatJmsMessage(jmsResponse) )
            
                emitRemoteServiceResponse(jmsResponse, suscriber)
                
                // Tell to the remote server that the JMS message has been successfully consumed
                jmsResponse.acknowledge()
                
                logger.debug(s"Message #$messageNumber ${jmsResponse.getJMSMessageID} has been successfully acknowledged !")
                
              } else {
                val errorMsg = s"Error while receiving message #$messageNumber ${jmsResponse.getJMSMessageID} from service '$serviceName' !"
                logger.error(errorMsg)
                
                throw new Exception(errorMsg)
              }
            } catch {
              case t: Throwable => {
                logger.error("Error during the processing of the remote service response",t) //return Observable.error(t)
                suscriber.onError(t)
              }
            } finally {
              // Important: don't forget to close the response consumer !
              // Otherwise other consumer instances may consume the messages sent by this producer
              
              val closeMsg = s"Closing the response consumer of message #$messageNumber ${jmsResponse.getJMSMessageID} from service '$serviceName'"

              if (synchronous) {
                logger.debug(closeMsg)
                responseConsumer.close()
              } else {
                Future {
                  // Important: don't forget to close the response consumer !
                  // Otherwise other consumer instances may consume the messages sent by this producer
                  logger.debug(closeMsg)
                  responseConsumer.close()
                } (callerCtx.execCtx) // (scala.concurrent.ExecutionContext.Implicits.global)*/
              }
            }
            
            // Complete the subscriber
            suscriber.onCompleted()
            
          } (callerCtx.execCtx)
        }
      }
      
      // Asynchronous message reception handling
      if (synchronous == false)
        responseConsumer.setMessageListener(messageListener)
  
      // Send the Message to the remote service using a synchronous send
      // For asynchronous send see: http://www.oracle.com/technetwork/articles/java/jms2messaging-1954190.html
      //val latch = new CountDownLatch(1)
      //val completionListener = new RemoteServiceMessageCompletionListener(latch)
      callerCtx.jmsProducer.send(callerCtx.jmsQueue, jmsRequest)
      
      // Wait until the reply has been received from the server
      //latch.await()
      
      // Snchronous message reception handling
      logger.debug(s"JMS Message ${jmsRequest.getJMSMessageID()} has been sent by the RemoteServiceCaller.")
      
      promise.success(jmsRequest)
      
      if (synchronous) {
        val jmsResponse = responseConsumer.receive()
        messageListener.onMessage(jmsResponse)
      }

    }
    
    (promise.future, observable.subscribeOn(callerCtx.execCtxScheduler)) //.observeOn(callerCtx.execCtxScheduler)
  }

  protected def addServiceClientToMessage(message: Message)(implicit callerCtx: RemoteServiceCallerContext): Unit = {

    val clientName = callerCtx.clientName.getOrElse("UNKNWON")
    val ipAddress = InetAddress.getLocalHost().getHostAddress()
    val source = s"Client '$clientName' hosted by '$ipAddress'"

    message.setStringProperty(JMSConstants.PROLINE_SERVICE_SOURCE_KEY, source)
  }
  
  /**
   * Abstract method used to parse the remote service response.
   * @param jmsMessage The JMS message to parse.
   */
  protected def mkServiceErrorMsg(errorMsg: String): String = {
    serviceName + " service error: " + errorMsg
  }
  
  protected def newServiceException(errorMsg: String): Exception = {
    new Exception(this.mkServiceErrorMsg(errorMsg) )
  }


}

/*private[this] class RemoteServiceMessageCompletionListener(latch: CountDownLatch) extends CompletionListener {

  var exception: Exception = null
  def getException() = exception

  override def onCompletion(message: Message) {
    latch.countDown()
  }

  override def onException(message: Message, exception: Exception) {
    latch.countDown()
    this.exception = exception
  }

}*/