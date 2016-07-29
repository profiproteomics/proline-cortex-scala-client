package fr.proline.cortex.client.sample

import javax.jms.Connection
import javax.jms.Session

import com.typesafe.scalalogging.StrictLogging

import org.hornetq.api.core.TransportConfiguration
import org.hornetq.api.jms.HornetQJMSClient
import org.hornetq.api.jms.JMSFactoryType
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory

import fr.proline.cortex.api.CortexConstants._
import fr.proline.cortex.api.JMSConnectionConfig
import fr.proline.cortex.client._
import fr.proline.jms.util.JMSConstants._

object MonitoringTopicSubscriberExample extends StrictLogging {

  val NettyConnectorFactoryClassName = classOf[NettyConnectorFactory].getName()

  /**
   * @param args
   */
  def main(args: Array[String]) {

    val jmsConnConfig = JMSConnectionConfig()
    var connection: Connection = null
    var session: Session = null

    try {
      // Step 1. Directly instantiate the JMS Topic object.
      val topic = HornetQJMSClient.createTopic(SERVICE_MONITORING_NOTIFICATION_TOPIC_NAME)

      // Step 2. Instantiate the TransportConfiguration object 
      // which contains the knowledge of which transport to use, the server port etc.

      val connectionParams = jmsConnConfig.toTransportConfigParams()

      val transportConfiguration = new TransportConfiguration(
        NettyConnectorFactoryClassName, connectionParams
      )

      // Step 3. Directly instantiate the JMS ConnectionFactory object using that TransportConfiguration
      val cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration)

      // Step 4. Create a JMS Connection
      connection = cf.createConnection()

      /* Thread specific : Session, Producer, Consumer ... */
      // Step 5. Create a JMS Session (Session MUST be confined in current Thread)
      // Not transacted, AUTO_ACKNOWLEDGE
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

      // Step 6. Create the subscription and the subscriber.
      val messageConsumer = session.createConsumer(topic)

      messageConsumer.setMessageListener(BasicMessageListener)

      // Explicitely start connection to begin Consumer reception
      logger.debug("Starting Message reception...")
      connection.start()

      ConsoleUtils.waitUserInput()
      println("Message reception stopped by user input")

    } catch {
      case ex: Exception => logger.error("Error sending JMS Message", ex)
    } finally {

      if (session != null) {
        try {
          session.close()
          logger.info("JMS Session closed")
        } catch {
          case exClose: Exception => logger.error("Error closing JMS Session", exClose)
        }
      }

      if (connection != null) {
        try {
          connection.close()
          logger.info("JMS Connection closed")
        } catch {
          case exClose: Exception => logger.error("Error closing JMS Connection", exClose)
        }
      }

    }

    println("\nMain terminated !")
  }

}
