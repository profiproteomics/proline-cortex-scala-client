package fr.proline.cortex.client.context

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import javax.jms.JMSContext
import javax.jms.Session

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
    
    // Create a single thread executor that will be used for JMS messages processing
    val singleThreadExecutor = Executors.newSingleThreadExecutor
    implicit val execCtx = ExecutionContext.fromExecutor(singleThreadExecutor)
    
    val futureResult = Future {
      // Create a new CLIENT_ACKNOWLEDGE JMS Context (JMS Context and its underlying Session MUST be confined in current Thread)
      val serviceCallerJmsCtx = managedJmsContext.createContext(Session.CLIENT_ACKNOWLEDGE)
      new RemoteServiceCallerContext(serviceCallerJmsCtx, queueName, singleThreadExecutor)
    }
    
    Await.result(futureResult, Duration.Inf)
  }
}

class RemoteServiceCallerContext private[context](
  val jmsContext: JMSContext,
  val queueName: String,
  val threadExecutor: ExecutorService
)(implicit val execCtx: ExecutionContext) extends StrictLogging {
  
  logger.info(s"Using JMS Queue '$queueName'")
  
  val execCtxScheduler = ExecutionContextScheduler(execCtx)
  
  // Instantiate the Proline remote service JMS Queue
  val jmsQueue = jmsContext.createQueue(queueName)
  
  // Create a JMS Message Producer (Producer MUST be confined in current Thread)
  val jmsProducer = jmsContext.createProducer()
  
  // Create a temporary queue for the replies consumption
  val temporaryReplyQueue = jmsContext.createTemporaryQueue()
  
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

