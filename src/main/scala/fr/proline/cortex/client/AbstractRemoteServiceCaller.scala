package fr.proline.cortex.client

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
   * Else the JMS response is parsed using the ''parseRemoteServiceResponse'' method.
   */
  protected def callRemoteService(jmsRequest: Message)(implicit callerCtx: RemoteServiceCallerContext): Observable[T] = {
    
    // Add some header properties to target the appropriate service and its corresponding version 
    jmsRequest.setStringProperty(JMSConstants.PROLINE_SERVICE_NAME_KEY, serviceName)
    jmsRequest.setStringProperty(JMSConstants.PROLINE_SERVICE_VERSION_KEY, serviceVersion)
    
    val observer = Observable[T] { suscriber =>
      
      val startingTime = System.currentTimeMillis()
      
      logger.info(s"RemoteServiceCaller will send a JMS request for service '$serviceName'")
      
      //val latch = new CountDownLatch(1)
      
      // Create new MessageConsumer that will be used to listen for the remote service response
      // TODO: find a way to maintain the responseConsumer active ?
      var responseConsumer = callerCtx.jmsContext.createConsumer(callerCtx.temporaryReplyQueue)
      
      var messageListener = new Object with MessageListener {
        override def onMessage(jmsResponse: Message) {
          Future {
            try {
              val endingTime = System.currentTimeMillis()
              val requestDuration = (endingTime - startingTime) / 60
              logger.debug(s"Received JMS response from remote service '$serviceName' after $requestDuration seconds.")
              
              val messageNumber = messagesCountSequence.incrementAndGet()
              
              if (jmsResponse != null) {
                logger.debug(s"Received message #$messageNumber ${jmsResponse.getJMSMessageID} from remote service '$serviceName'")
                logger.debug(s"Content of message ${jmsResponse.getJMSMessageID} is:" + JMSUtils.formatJmsMessage(jmsResponse) )
            
                emitRemoteServiceResponse(jmsResponse, suscriber)
                
                // Tell to the remote server that the JMS message has been successfully consumed
                jmsResponse.acknowledge()
                
              } else {
                val errorMsg = s"Error while receiving message #$messageNumber from service '$serviceName' !"
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
              responseConsumer.close()
              responseConsumer = null
            }
            
            suscriber.onCompleted()
            
          } (callerCtx.execCtx)
        }
      }
      
      responseConsumer.setMessageListener(messageListener)
      
      // Tell the remote service to reply to a particular queue: a TemporaryQueue used to store Server->Client responses
      jmsRequest.setJMSReplyTo(callerCtx.temporaryReplyQueue)
  
      // Send the Message to the remote service using a synchronous send
      // For asynchronous send see: http://www.oracle.com/technetwork/articles/java/jms2messaging-1954190.html
      //val latch = new CountDownLatch(1)
      //val completionListener = new RemoteServiceMessageCompletionListener(latch)
      callerCtx.jmsProducer.send(callerCtx.jmsQueue, jmsRequest)
      
      logger.debug(s"JMS Message ${jmsRequest.getJMSMessageID()} has been sent by the RemoteServiceCaller.")
      
      // Wait until the reply has been received from the server
      //latch.await()
    }
    
    observer.subscribeOn(callerCtx.execCtxScheduler) //.observeOn(callerCtx.execCtxScheduler)
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