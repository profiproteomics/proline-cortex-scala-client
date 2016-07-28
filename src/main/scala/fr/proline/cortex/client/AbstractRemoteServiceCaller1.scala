package fr.proline.cortex.client

/*
import java.util.Map
import javax.jms._
import com.thetransactioncompany.jsonrpc2._
import com.typesafe.scalalogging.StrictLogging
import fr.profi.util.misc.InMemoryIdGen
import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.JMSNotificationMessage
import java.util.concurrent.atomic.AtomicInteger

object RemoteServiceCaller extends InMemoryIdGen {
  //val TASK_LIST_INFO = "JMS Services"
}

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
abstract class AbstractRemoteServiceCaller(
  // callback is called by the AccessServiceThread when the service is done
  val callback: Option[(Boolean) => Unit] = None,
  val synchronous: Boolean = true,
  var responseTimeout: Int = 10000
) extends MessageListener with StrictLogging {
  
  protected var _producer: MessageProducer = null
  protected var _replyQueue: TemporaryQueue = null
  protected var _responseConsumer: MessageConsumer = null
  
  //protected var id = RemoteServiceCaller.generateNewId()
  private var _startingTime = -1L
  
  /* To count received messages */
  private val messagesCountSequence = new AtomicInteger(0)
  
  /**
   * Called when the task must be started.
   * The implementation should call setTaskInfoRequest to register request information.
   * 
   * @throws JMSException
   */
  def runRemoteService(): Unit
  
  /*def setTaskInfoRequest(content: String): Unit = {
    _taskInfo.setRequestURL(_producer.getDestination().toString())
    _taskInfo.setRequestContent(content)
  }*/
  
  /**
   * Called when the task is done
   * @param jmsMessage
   * @throws Exception 
   */
  def onRemoteServiceDone(jmsMessage: Message): Unit

  /**
   * Method called by the AccessJMSManagerThread to ask for the service to be done     
   * @throws javax.jms.JMSException
   */
  def callRemoteService() {
    _startingTime = System.currentTimeMillis()
    
    val jsmConnManager = JMSConnectionManager.getInstance()
    require( jsmConnManager.isConnected(), "JMSConnectionManager is not connected !")

    /*
     * Thread specific : Session, Producer, Consumer ...
     */
    // Get JMS Session (Session MUST be confined in current Thread)            
    val jmsSessionOpt = jsmConnManager.getRemoteServiceJMSSession()
    require( jmsSessionOpt.isDefined, "jmsSessionOpt is not defined" )
    
    val jmsSession = jmsSessionOpt.get

    // Create a JMS Message Producer (Producer MUST be confined in current Thread)
    _producer = jmsSession.createProducer(jsmConnManager.getServiceQueue().get)
    _replyQueue = jmsSession.createTemporaryQueue()
    _responseConsumer = jmsSession.createConsumer(_replyQueue)
    
    if (synchronous == false) {
      _responseConsumer.setMessageListener(this)
    }

    //this.currentJMSState = JMSState.STATE_WAITING
    
    // Run the remote service
    this.runRemoteService()
    
    // Wait for the message if synchronous mode is enabled
    if (synchronous) {
      this.onMessage(_responseConsumer.receive(responseTimeout))
    }
  
  }

  /**
   * Method called by the ServiceStatusThread
   * to check if the service is done
   * @return current JMS Task State (one of AbstractJMSTask.JMSState)
   */
  //def getJMSState(): JMSState = currentJMSState
  
  override def onMessage(jmsMessage: Message) {
      
    val endingTime = System.currentTimeMillis()
    val requestDuration = (endingTime - _startingTime) / 60
    logger.debug(s"Received JMS response after $requestDuration seconds.")
    
    val messageNumber = this.messagesCountSequence.incrementAndGet()
    
    if (jmsMessage != null) {
      logger.debug(s"Received message #$messageNumber: " + JMSUtils.formatJmsMessage(jmsMessage))
  
      onRemoteServiceDone(jmsMessage)
      
      callback.map( _.apply(true) )
      
      // Tell to that the JMS message has been successfully consumed
      jmsMessage.acknowledge()
      
    } else {
      val errorMsg = s"Error while receiving message #$messageNumber, timeout may have occured"
      logger.error(errorMsg)
      
      callback.map( _.apply(false) )
      
      throw new Exception(errorMsg)
    }
    
  }

}*/
