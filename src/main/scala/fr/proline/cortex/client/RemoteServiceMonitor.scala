package fr.proline.cortex.client

import javax.jms._

import com.thetransactioncompany.jsonrpc2.JSONRPC2Notification
import com.typesafe.scalalogging.StrictLogging

import org.hornetq.api.jms.HornetQJMSClient

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.CortexConstants._
import fr.proline.cortex.api.JMSNotificationMessage
import fr.proline.jms.util.JMSConstants
import fr.proline.jms.util.JMSUtils

object RemoteServiceMonitor {
  
  private var _remoteServiceMonitor: RemoteServiceMonitor = null
  
  def apply()(implicit jmsContext: JMSContext): RemoteServiceMonitor = synchronized {
    _remoteServiceMonitor = new RemoteServiceMonitor()
    _remoteServiceMonitor
  }
  
  def getInstance(): RemoteServiceMonitor = {
    require( _remoteServiceMonitor != null, "The RemoteServiceMonitor has not been initiated yet !" )
    _remoteServiceMonitor
  }
  
  def close() {
    if (_remoteServiceMonitor != null) {
      _remoteServiceMonitor.close()
    }
  }
}


class RemoteServiceMonitor private()(implicit private val _managedJmsContext: JMSContext) extends StrictLogging {
 
  private val _notifListener: ServiceNotificationListener = new ServiceNotificationListener()
  private val _notifListener_lock = new Object()
  
  // Create a new AUTO_ACKNOWLEDGE JMS Context (JMS Context and its underlying Session MUST be confined in current Thread)
  private val jmsMonitoringContext = _managedJmsContext.createContext(Session.AUTO_ACKNOWLEDGE)
  
  private val jmsMonitoringConsumer: JMSConsumer = {
    
    // Create the subscriber and attach the message listener
    val topicSuscriber = jmsMonitoringContext.createConsumer(
      HornetQJMSClient.createTopic(JMSConstants.SERVICE_MONITORING_NOTIFICATION_TOPIC_NAME)
    )
    topicSuscriber.setMessageListener(_notifListener)
    
    topicSuscriber
  }
  
  def setNotificationCallback(callback: (JMSNotificationMessage) => Unit): Unit = {
    _notifListener_lock.synchronized {
      _notifListener.callback = Some(callback)
    }
  }

  def close() = synchronized {

    try {
      jmsMonitoringConsumer.close()
      jmsMonitoringContext.close()

      logger.info("RemoteServiceMonitor successfully closed !")
    } catch {
      case closeConnError: Exception => {
        logger.error("Error closing RemoteServiceMonitor !")
        throw closeConnError
      }
    }

  }


}

private [this] class ServiceNotificationListener(
  var callback: Option[(JMSNotificationMessage) => Unit] = None
) extends MessageListener with StrictLogging {

  override def onMessage(jmsMessage: Message) {
    logger.debug(s"Notification Listener receiving message ${jmsMessage.getJMSMessageID }")
    logger.debug(s"Content of message ${jmsMessage.getJMSMessageID} is:" + JMSUtils.formatJmsMessage(jmsMessage) )

    if (callback.isEmpty) return
    
    jmsMessage match {
      case textMessage: TextMessage => {

        try {
          val jsonString = textMessage.getText()
          val jsonNotif = JSONRPC2Notification.parse(jsonString)
          val params = jsonNotif.getNamedParams()

          val msg = ProfiJson.getObjectMapper().convertValue(params, classOf[JMSNotificationMessage])
          callback.get.apply(msg)

        } catch {
          case e: Exception => {
            logger.error("Error handling JMS Message", e)
            throw new Exception("Error handling JMS Message", e)
          }
        }
      }
      case _ => {
        logger.warn("Invalid JMS Message type")
      }
    }
  }

}
