package fr.proline.cortex.client.sample

import java.util.Enumeration
import java.util.HashMap
import java.util.Map

import javax.jms.ConnectionFactory
import javax.jms.JMSContext
import javax.jms.Message
import javax.jms.Queue
import javax.jms.QueueBrowser

import org.hornetq.api.core.TransportConfiguration
import org.hornetq.api.jms.HornetQJMSClient
import org.hornetq.api.jms.JMSFactoryType
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory
import org.hornetq.core.remoting.impl.netty.TransportConstants
import com.typesafe.scalalogging.StrictLogging

import fr.proline.cortex.api.CortexConstants._
import fr.proline.cortex.api.JMSConnectionConfig
import fr.proline.cortex.client._
import fr.proline.jms.util.JMSConstants._
import fr.proline.jms.util.JMSUtils

/* QueueMonitor use QueueBrowser with new JMS 2.0 JMSContext
 * Shows all active massage staying in Proline JMS Queue */
object QueueMonitor extends StrictLogging {

  val jmsConnConfig = JMSConnectionConfig()
  val NettyConnectorFactoryClassName = classOf[NettyConnectorFactory].getName()

  def main(args: Array[String]) {
    var jmsContext: JMSContext = null

    try {
      // Step 2. Instantiate the TransportConfiguration object 
      // which contains the knowledge of which transport to use, the server port etc.

      val connectionParams = jmsConnConfig.toTransportConfigParams()

      val transportConfiguration = new TransportConfiguration(
        NettyConnectorFactoryClassName, connectionParams
      )

      // Step 3. Directly instantiate the JMS ConnectionFactory object using that TransportConfiguration
      val cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration)

      println("Cf : " + cf)

      // Create a JMS v 2.0 Context
      jmsContext = cf.createContext()

      // Directly instantiate the JMS Queue object.
      val queue = jmsContext.createQueue(DEFAULT_PROLINE_SERVICE_REQUEST_QUEUE_NAME)

      println("Browsing  " + queue)
      println()

      /* Thread specific : Session, Producer, Consumer ... */
      val queueBrowser = jmsContext.createBrowser(queue)

      val messageEnum = queueBrowser.getEnumeration()

      var nMessages = 0

      while (messageEnum.hasMoreElements()) {
        val msg = messageEnum.nextElement().asInstanceOf[Message]
        nMessages += 1

        System.out.println(JMSUtils.formatJmsMessage(msg))
      }

      if (nMessages == 0) {
        System.out.println("Queue is empty !")
      } else {
        System.out.println("Total messages count: " + nMessages)
      }

    } catch {
      case ex: Exception => logger.error("Error monitoring JMS Queue", ex)
    } finally {

      if (jmsContext != null) {
        try {
          jmsContext.close()
          logger.info("JMS Context closed")
        } catch {
          case exClose: Exception => logger.error("Error closing JMS Context", exClose)
        }
      }

    }

    System.out.println("\nMain terminated !")
  }

}
