package fr.proline.cortex.client.context

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import javax.jms._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import com.typesafe.scalalogging.StrictLogging

import fr.proline.cortex.api.CortexConstants
import fr.proline.cortex.api.JMSConnectionConfig
import fr.proline.cortex.caller.IServiceCallerContext

import rx.lang.scala.schedulers.ExecutionContextScheduler

object BuildJMSServiceCallerContext extends StrictLogging {
  
  def apply(
    clientName: Option[String] = None,
    connectionConfig: JMSConnectionConfig,
    queueName: String = CortexConstants.DEFAULT_PROLINE_SERVICE_REQUEST_QUEUE_NAME
  )(implicit managedJmsContext: JMSContext): JMSServiceCallerContext = {
    
    logger.info(s"Using JMS Queue '$queueName'")
    
    // Create a single thread executor that will be used for JMS messages processing
    implicit val singleThreadExecutor = Executors.newSingleThreadExecutor
    implicit val execCtx = ExecutionContext.fromExecutor(singleThreadExecutor)
    
    this.withThreadExecutor(clientName, connectionConfig, queueName)
  }
  
  def withThreadExecutor(
    clientName: Option[String] = None,
    connectionConfig: JMSConnectionConfig,
    queueName: String = CortexConstants.DEFAULT_PROLINE_SERVICE_REQUEST_QUEUE_NAME
  )(
    implicit managedJmsContext: JMSContext,
    threadExecutor: ExecutorService,
    execCtx: ExecutionContext
  ): JMSServiceCallerContext = {
    
    // The JMSContext and its related objects (Session, Producer, Consumer) MUST be confined in a single Thread
    // Thus we create the RemoteServiceCallerContext in a Future executed in the singleThreadExecutor previously instantiated
    // Important: this single Thread ExecutorService must be shut down by calling the close() method of the RemoteServiceCallerContext
    val futureResult = Future {
      logger.debug("Creating new remote service caller JMSContext from thread: " + Thread.currentThread().getName() )
      
      // Create a new CLIENT_ACKNOWLEDGE JMS Context (JMS Context and its underlying Session MUST be confined in current Thread)
      val serviceCallerJmsCtx = managedJmsContext.createContext(Session.CLIENT_ACKNOWLEDGE)
      
      // Instantiate the Proline remote service JMS Queue
      val jmsQueue = serviceCallerJmsCtx.createQueue(queueName)
      
      // Create a JMS Message Producer (Producer MUST be confined in current Thread)
      val jmsProducer = serviceCallerJmsCtx.createProducer()
      
      val jmsScCtx = new JMSServiceCallerContext(clientName, connectionConfig)
      
      jmsScCtx._init(
        serviceCallerJmsCtx,
        jmsQueue,
        jmsProducer
      )
      
      jmsScCtx
    }
    
    Await.result(futureResult, Duration.Inf)
  }
  
}

case class JMSServiceCallerContext private[context] (clientName: Option[String], connectionConfig: JMSConnectionConfig)(
  implicit val execCtx: ExecutionContext,
  implicit private val threadExecutor: ExecutorService,
  implicit private val managedJmsContext: JMSContext
) extends IServiceCallerContext with StrictLogging {
  
  private var _jmsContext: JMSContext = _
  private var _jmsQueue: Queue = _
  private var _jmsProducer: JMSProducer = _
  
  def jmsContext: JMSContext = _jmsContext
  def jmsQueue: Queue = _jmsQueue
  def jmsProducer: JMSProducer = _jmsProducer
  
  private[context] def _init(
    jmsContext: JMSContext,
    jmsQueue: Queue,
    jmsProducer: JMSProducer
  ) = this.synchronized {
    _jmsContext = jmsContext
    _jmsQueue = jmsQueue
    _jmsProducer = jmsProducer
  }
  
  val execCtxScheduler = ExecutionContextScheduler(execCtx)

  def close() {
    jmsContext.close()
    threadExecutor.shutdown()
  }
  
  def reconnect() {
    try {
      jmsContext.close()
      managedJmsContext.close()
      logger.info("Failed JMS connection successfully closed!")
    } catch {
      case t: Throwable => logger.error("Could not close previous JMS connection properly",t) 
    }
    
    logger.info("Trying to reconnect to JMS server...")
    val newJmsScCtx = BuildJMSServiceCallerContext.withThreadExecutor(
      clientName,
      connectionConfig, 
      jmsQueue.getQueueName
    )(BuildJMSContext(connectionConfig), threadExecutor, execCtx)
    
    this._init(
      newJmsScCtx.jmsContext,
      newJmsScCtx.jmsQueue,
      newJmsScCtx.jmsProducer
    )
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
  
}
