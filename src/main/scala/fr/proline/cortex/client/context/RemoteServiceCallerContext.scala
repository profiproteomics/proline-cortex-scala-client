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

import rx.lang.scala.schedulers.ExecutionContextScheduler

object BuildRemoteServiceCallerContext extends StrictLogging {
  
  def apply(
    queueName: String = CortexConstants.DEFAULT_PROLINE_SERVICE_REQUEST_QUEUE_NAME
  )(implicit managedJmsContext: JMSContext): RemoteServiceCallerContext = {
    
    logger.info(s"Using JMS Queue '$queueName'")
    
    // Create a single thread executor that will be used for JMS messages processing
    implicit val singleThreadExecutor = Executors.newSingleThreadExecutor
    implicit val execCtx = ExecutionContext.fromExecutor(singleThreadExecutor)
    
    // The JMSContext and its related objects (Session, Producer, Consumer) MUST be confined in a single Thread
    // Thus we create the RemoteServiceCallerContext in a Future executed in the singleThreadExecutor previously instantiated
    // Important: this single Thread ExecutorService must be shut down by calling the close() method of the RemoteServiceCallerContext
    val futureResult = Future {
      logger.debug("Creation new remote service caller JMSContext from thread: " + Thread.currentThread().getName() )
      
      // Create a new CLIENT_ACKNOWLEDGE JMS Context (JMS Context and its underlying Session MUST be confined in current Thread)
      val serviceCallerJmsCtx = managedJmsContext.createContext(Session.CLIENT_ACKNOWLEDGE)
      
      // Instantiate the Proline remote service JMS Queue
      val jmsQueue = serviceCallerJmsCtx.createQueue(queueName)
      
      // Create a JMS Message Producer (Producer MUST be confined in current Thread)
      val jmsProducer = serviceCallerJmsCtx.createProducer()
      
      new RemoteServiceCallerContext(
        serviceCallerJmsCtx,
        jmsQueue,
        jmsProducer
      )
    }
    
    Await.result(futureResult, Duration.Inf)
  }
}

case class RemoteServiceCallerContext private[context](
  val jmsContext: JMSContext,
  val jmsQueue: Queue,
  val jmsProducer: JMSProducer
)(implicit val execCtx: ExecutionContext, implicit private val threadExecutor: ExecutorService) extends StrictLogging {
  
  val execCtxScheduler = ExecutionContextScheduler(execCtx)
  
  def close() {
    jmsContext.close()
    threadExecutor.shutdown()
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
