package fr.proline.cortex.client.sample

import java.util.HashMap
import java.util.Map
import java.util.concurrent.TimeUnit

import javax.jms.Connection
import javax.jms.ConnectionFactory
import javax.jms.Destination
import javax.jms.Message
import javax.jms.MessageConsumer
import javax.jms.MessageProducer
import javax.jms.Queue
import javax.jms.Session
import javax.jms.TextMessage

import org.hornetq.api.core.TransportConfiguration
import org.hornetq.api.jms.HornetQJMSClient
import org.hornetq.api.jms.JMSFactoryType
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory
import org.hornetq.core.remoting.impl.netty.TransportConstants

import com.thetransactioncompany.jsonrpc2.JSONRPC2Error
import com.thetransactioncompany.jsonrpc2.JSONRPC2Response
import com.typesafe.scalalogging.StrictLogging

import fr.profi.util.StringUtils.LINE_SEPARATOR
import fr.proline.cortex.api.CortexConstants._
import fr.proline.cortex.client._
import fr.proline.jms.util.JMSConstants._

/* Purge old messages from Proline JMS Queue */
object Purge extends StrictLogging {
  
  val NettyConnectorFactoryClassName = classOf[NettyConnectorFactory].getName()

  private val PURGE_BEFORE = TimeUnit.HOURS.toMillis(1) // 1 hour

  def main(args: Array[String]) {
    
    val jmsConnConfig = JMSConnectionConfig()
    var connection: Connection = null
    var session: Session = null

    try {
      // Step 1. Directly instantiate the JMS Queue object.
      val queue = HornetQJMSClient.createQueue(DEFAULT_PROLINE_SERVICE_REQUEST_QUEUE_NAME)

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

      val originTimestamp = System.currentTimeMillis() - PURGE_BEFORE
      logger.debug("Purging JMS Message before " + JMSUtils.DATE_FORMAT.format(originTimestamp))

      val selectorString = "JMSTimestamp < " + originTimestamp
      logger.debug("Consumer selector [" + selectorString + ']')

      // Step 6. Create a JMS Message Producer (Producer MUST be confined in current Thread)
      val consumer = session.createConsumer(queue, selectorString)

      /*
	     * ReplyProducer to send JMS Response Message to Client (Producer MUST be confined in current
	     * Thread)
	     */
      val replyProducer = session.createProducer(null)

      connection.start() // Explicitely start connection to begin Consumer reception

      var goOn = true

      while (goOn) {
        // Block max. 5 seconds
        val message = consumer.receive(TimeUnit.SECONDS.toMillis(5))

        if (message == null) {
          goOn = false
          logger.info("Exiting MessageConsumer.receive() loop")
        } else {

          try {
            val jmsMessageId = message.getJMSMessageID()

            logger.info("Purging JMS Message" + LINE_SEPARATOR + JMSUtils.formatJmsMessage(message))

            val replyDestination = message.getJMSReplyTo()

            if (replyDestination == null) {
              logger.warn("Message has no JMSReplyTo Destination : Cannot send JSON Response to Client")
            } else {
              /* Try to send a JSON-RPC Error to client Producer */
              val jsonError = new JSONRPC2Error(
                JSONRPC2Error.INTERNAL_ERROR.getCode(), "Purging OLD JMS message !!")
              val jsonResponse = new JSONRPC2Response(jsonError, null)

              // Step 7. Create a Text Message
              val jmsResponseMessage = session.createTextMessage()
              jmsResponseMessage.setJMSCorrelationID(jmsMessageId)
              jmsResponseMessage.setText(jsonResponse.toJSONString())

              logger.debug("Sending JMS Response to Message [" + jmsMessageId + "] on Destination [" + replyDestination + ']')

              replyProducer.send(replyDestination, jmsResponseMessage)
            }

          } catch {
            case ex: Exception => logger.error("Error purging JMS Message", ex)
          }

        }

      } // End loop

    } catch {
      case ex: Exception => logger.error("Error in loop purging JMS Messages", ex)
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

    System.out.println("\nMain terminated !")
  }

}
