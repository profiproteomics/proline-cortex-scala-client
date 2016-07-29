package fr.proline.cortex.client

import java.util.concurrent.atomic.AtomicInteger

import javax.jms._

import com.thetransactioncompany.jsonrpc2._
import com.typesafe.scalalogging.StrictLogging

import fr.proline.cortex.api._
import fr.proline.cortex.api.CortexConstants._
import fr.proline.cortex.client.context.RemoteServiceCallerContext
import fr.proline.jms.service.api.IRemoteServiceIdentity
import fr.proline.jms.util.JMSUtils

import rx.lang.scala.Observable

// TODO: create an implicit RemoteServiceExecutionContext ???
/*object RemoteServiceCaller extends StrictLogging {
  
  // Create a single thread executor that will be used for JMS messages processing
  private val singleThreadExecutor = Executors.newSingleThreadExecutor
  private val execCtx = ExecutionContext.fromExecutor(singleThreadExecutor)
  private val execCtxScheduler = ExecutionContextScheduler(execCtx)
  
  private val jsmConnManager = JMSConnectionManager.getInstance()
  require( jsmConnManager.isConnected(), "JMSConnectionManager is not connected !")
  
  // Get JMS Connection
  val jmsConnection = jsmConnManager.getJMSConnection().get
  
  // Instantiate the Proline remote service JMS Queue
  val remoteServiceJMSQueue: Queue = {
    
    // TODO: add to JMSConnectionConfig ???
    val queueName = DEFAULT_PROLINE_SERVICE_REQUEST_QUEUE_NAME    
    logger.info(s"Using JMS Queue '$queueName'")
    
    HornetQJMSClient.createQueue(queueName)
  }
  
  // Create the remote service JMS session (NOT transacted, CLIENT_ACKNOWLEDGE)
  val remoteServiceJMSSession = jmsConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
  
  val messageChannel = RemoteServiceMessageChannel( remoteServiceJMSSession, remoteServiceJMSQueue )
  
  def start() {
    // Explicitly start the JMS connection to begin consumer listening 
    jmsConnection.start()
  }
  
  def stop() {
    messageChannel.jmsSession.close()
    singleThreadExecutor.shutdown()
  }
  
  // To enable concurrent processing of messages, a ConnectionConsumer must be created
  // - https://www.novell.com/documentation/extend5/Docs/help/MP/jms/tutorial/connConsumer-1.htm
  // - http://www.precisejava.com/javaperf/j2ee/JMS.htm
  /*val connectionConsumer = jsmConnManager.getJMSConnection().get.createConnectionConsumer(
    jsmConnManager.getRemoteServiceJMSQueue().get, // destination
    "",   // messageSelector
    null, // sessionPool
    1000  // maxMessages
  )*/
  
  def callRemoteService(jmsRequest: Message): Observable[Message] = {
    
    val observer = Observable[Message] { suscriber =>
      
      logger.debug("New MessageListener from thread: " + Thread.currentThread().getName() )
      
      val messageListener = new Object with MessageListener {
        override def onMessage(jmsResponse: Message) {
          suscriber.onNext(jmsResponse)
          suscriber.onCompleted()
        }
      }
      
      // Create new MessageConsumer that will be used to listen for the remote service response
      val responseConsumer = messageChannel.jmsSession.createConsumer(messageChannel.replyQueue)
      responseConsumer.setMessageListener(messageListener)
      
      // Tell the remote service to reply to a particular queue: a TemporaryQueue used to store Server->Client responses
      jmsRequest.setJMSReplyTo(messageChannel.replyQueue)
  
      // Send the Message to the remote service
      messageChannel.messageProducer.send(jmsRequest)
      
      logger.debug(s"JMS Message ${jmsRequest.getJMSMessageID()} has been sent by the RemoteServiceCaller.")
    }
    
    observer.subscribeOn(execCtxScheduler)
  }
  
  /*def callRemoteServiceNow(jmsMessage: Message, responseTimeout: Int): Message = {
    
    val responseConsumer = messageChannel.jmsSession.createConsumer(messageChannel.replyQueue)
    
    // Send JMS Message
    RemoteServiceCaller2._sendJmsMessage(jmsMessage)
    
    // Wait for the message if synchronous mode is enabled
    responseConsumer.receive(responseTimeout)
  }*/
  
}

*/

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
   * Abstract method used to parse the remote service response.
   * @param jmsMessage The JMS message to parse.
   */
  protected def parseRemoteServiceResponse(jmsMessage: Message): T
  
  /**
   * Call the remote service by sending a JMS request (Message) and waiting for a JMS response (Message).
   * If the JMS response is null then an Exception is thrown.
   * Else the JMS response is parsed using the ''parseRemoteServiceResponse'' method.
   */
  protected def callRemoteService(jmsRequest: Message)(implicit callerCtx: RemoteServiceCallerContext): Observable[T] = {
    
    val startingTime = System.currentTimeMillis()
    
    logger.info(s"RemoteServiceCaller will send a JMS request for service '$serviceName'...")
    
    this._sendJmsRequest(jmsRequest).map { jmsResponse =>
      
      val endingTime = System.currentTimeMillis()
      val requestDuration = (endingTime - startingTime) / 60
      logger.debug(s"Received JMS response of service '$serviceName' after $requestDuration seconds.")
      
      val messageNumber = this.messagesCountSequence.incrementAndGet()
      
      if (jmsResponse != null) {
        logger.debug(s"Received message #$messageNumber ${jmsResponse.getJMSMessageID} from service '$serviceName'")
        logger.trace(s"Content of message ${jmsResponse.getJMSMessageID} is:" + JMSUtils.formatJmsMessage(jmsResponse) )
    
        // Tell to that the JMS message has been successfully consumed
        jmsResponse.acknowledge()
        
        parseRemoteServiceResponse(jmsResponse)
        
      } else {
        val errorMsg = s"Error while receiving message #$messageNumber from service '$serviceName' !"
        logger.error(errorMsg)
        
        throw new Exception(errorMsg)
      }
    }
    
  }
  
  private def _sendJmsRequest(jmsRequest: Message)(implicit callerCtx: RemoteServiceCallerContext): Observable[Message] = {
    
    val observer = Observable[Message] { suscriber =>
      
      logger.debug("New MessageListener from thread: " + Thread.currentThread().getName() )
      
      val messageListener = new Object with MessageListener {
        override def onMessage(jmsResponse: Message) {
          suscriber.onNext(jmsResponse)
          suscriber.onCompleted()
        }
      }
      
      // Create new MessageConsumer that will be used to listen for the remote service response
      val responseConsumer = callerCtx.jmsContext.createConsumer(callerCtx.temporaryReplyQueue)
      responseConsumer.setMessageListener(messageListener)
      
      // Tell the remote service to reply to a particular queue: a TemporaryQueue used to store Server->Client responses
      jmsRequest.setJMSReplyTo(callerCtx.temporaryReplyQueue)
  
      // Send the Message to the remote service
      callerCtx.jmsProducer.send(callerCtx.jmsQueue, jmsRequest)
      
      logger.debug(s"JMS Message ${jmsRequest.getJMSMessageID()} has been sent by the RemoteServiceCaller.")
    }
    
    observer.subscribeOn(callerCtx.execCtxScheduler)
  }
  
  /**
   * Abstract method used to parse the remote service response.
   * @param jmsMessage The JMS message to parse.
   */
  protected def mkServiceErrorMsg(errorMsg: String): String = {
    serviceName + " service error: " + errorMsg
  }


}
