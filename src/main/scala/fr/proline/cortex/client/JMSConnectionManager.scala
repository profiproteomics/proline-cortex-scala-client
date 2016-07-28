package fr.proline.cortex.client

import javax.jms._
import com.typesafe.scalalogging.StrictLogging
import org.hornetq.api.jms.HornetQJMSClient
import org.hornetq.core.remoting.impl.netty.TransportConstants
import fr.profi.util.StringUtils
import fr.proline.jms.util.JMSConstants._
import org.hornetq.api.core.TransportConfiguration
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory
import org.hornetq.api.jms.JMSFactoryType
import fr.proline.cortex.api.JMSNotificationMessage
import fr.proline.cortex.api.CortexConstants._

// TODO: do not use a singleton (change the constructor of the JMSConnection to public and create an implicit JMSConnectionContext)
object JMSConnectionManager {
  
  private var _connectionManager: JMSConnectionManager = null
  
  def apply( config: JMSConnectionConfig ): JMSConnectionManager = synchronized {
    _connectionManager = new JMSConnectionManager(config)
    _connectionManager
  }
  
  def getInstance(): JMSConnectionManager = {
    require( _connectionManager != null, "The JMSConnectionManager has not been initiated yet !" )
    _connectionManager
  }
  
  def close() {
    if (_connectionManager != null) {
      _connectionManager.close()
    }
  }
}

case class JMSManagedConsumer(
  session: Session,
  messageConsumer: MessageConsumer
) {
  def close() {
    messageConsumer.close()
    session.close()
  }
}

/**
 * Manage the JMS connection/session for the Scala client.
 * For detailed information about JMS principles see: http://www.precisejava.com/javaperf/j2ee/JMS.htm
 */
class JMSConnectionManager private [client]( config: JMSConnectionConfig ) extends StrictLogging {
  require( StringUtils.isNotEmpty(config.serverHost), "serverHost not defined !")
 
  private var _notifListener: Option[ServiceNotificationListener] = None
  private val _notifListener_lock = new Object()
  
  private var _jmsContext: Option[JMSContext] = None
  private var _jmsConnection: Option[Connection] = Some(_createJMSConnection)
  private var _jmsMonitoringTopic: Option[JMSManagedConsumer] = _createMonitoringTopic()
  
  def isConnected() = _jmsConnection.isDefined
  
  def getJMSConnection(): Option[Connection] = _jmsConnection
  
  //def getJMSContext(): Option[JMSContext] = _jmsContext
  
  // TODO: replace by setMessageListener and add the messageListener without modifying it here
  def setNotificationCallback(callback: (JMSNotificationMessage) => Unit): Unit = {
    require(this.isConnected() && _notifListener.isDefined, "JMSConnectionManager is disconnected !")
    
    _notifListener_lock.synchronized {
      _notifListener.get.callback = Some(callback)
    }
  }
  
  private def _createJMSConnection(): Connection = {
    
    // Instantiate the TransportConfiguration object
    // which contains the knowledge of which transport to use, the server port etc.
    val transportConfigParams = config.toTransportConfigParams()

    val transportConfiguration = new TransportConfiguration(
      classOf[NettyConnectorFactory].getName(),
      transportConfigParams
    )

    // Instantiate the JMS ConnectionFactory object using this TransportConfiguration
    val connFactory = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration)
      
    // Create a JMS v2.0 Context
    _jmsContext = Some(connFactory.createContext())
    
    try {
      // Create a JMS Connection
      connFactory.createConnection()
    } catch {
      case newConnError: Exception => {
        val errorMsg = "JMS connection creation failed !"
        logger.error(errorMsg)
        throw new Exception(errorMsg, newConnError )
      }
    }

  }
  
  private def _createMonitoringTopic(): Option[JMSManagedConsumer] = {
    if (_jmsConnection.isEmpty) return None
    
    // Create a JMS Session (Session MUST be confined in current Thread)
    // Not transacted, AUTO_ACKNOWLEDGE
    val topicSession = this._jmsConnection.get.createSession(false, Session.AUTO_ACKNOWLEDGE)
    
    // Create a message listener
    val notifListener = new ServiceNotificationListener()
    this._notifListener = Some(notifListener)
    
    // Create the subscriber and attach the message listener
    val topicSuscriber = topicSession.createConsumer(
      HornetQJMSClient.createTopic(SERVICE_MONITORING_NOTIFICATION_TOPIC_NAME)
    )
    topicSuscriber.setMessageListener(notifListener)
    
    Some(
      JMSManagedConsumer(
        session = topicSession,
        messageConsumer = topicSuscriber
      )
    )
  }

  def close() = synchronized {

    if (_jmsConnection.isDefined) {
      try {
        if (_jmsMonitoringTopic.isDefined)
          _jmsMonitoringTopic.get.close()
        
        _jmsConnection.get.close()

        logger.info("JMS connection successfully closed !")
      } catch {
        case closeConnError: Exception => {
          logger.error("Error closing JMS Connection !")
          throw closeConnError
        }
      }
      finally {
        // Reset private vars
        _jmsContext = None
        _jmsConnection = None
        _jmsMonitoringTopic = None
        _notifListener = None
      }
    }

  }


}
