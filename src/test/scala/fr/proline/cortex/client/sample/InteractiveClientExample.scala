package fr.proline.cortex.client.sample

import java.util.HashMap
import java.util.UUID

import javax.jms.Connection
import javax.jms.Session

import com.thetransactioncompany.jsonrpc2.JSONRPC2Request
import com.typesafe.scalalogging.StrictLogging

import org.hornetq.api.core.TransportConfiguration
import org.hornetq.api.jms.HornetQJMSClient
import org.hornetq.api.jms.JMSFactoryType
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory

import fr.proline.cortex.api.CortexConstants._
import fr.proline.cortex.api.JMSConnectionConfig
import fr.proline.cortex.api.service.misc.FileSystemService
import fr.proline.cortex.api.service.misc.FileSystemService.RETRIEVE_DIRECTORY_CONTENT_METHOD
import fr.proline.cortex.client._
import fr.proline.jms.util.JMSConstants._

/* Sample Consumer Client with JMS 1.1 Connection and Session 
 * Send a JSON-RPC Request and receive a JSON-RPC Response on TemporaryQueue */
object InteractiveClientExample extends StrictLogging {

  val INFO_SERVICE_NAME = "proline/monitoring/Info"

  val SINGLE_THREADED_INFO_SERVICE_NAME = "proline/monitoring/SingleThreadedInfo"

  val SERVICE_VERSION = "1.0"

  val FAKE_MOUNT_POINT = "fake_for_test"

  val N_MESSAGES = 1

  val NettyConnectorFactoryClassName = classOf[NettyConnectorFactory].getName()

  /**
   * @param args
   */
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

      // Step 6. Create a JMS Message Producer (Producer MUST be confined in current Thread)
      val messageProducer = session.createProducer(queue)

      val replyTmpQueue = session.createTemporaryQueue()

      val responseConsumer = session.createConsumer(replyTmpQueue)
      responseConsumer.setMessageListener(BasicMessageListener)

      // Explicitely start connection to begin Consumer reception
      connection.start()

      // Step 7. Create a Text Message

      if (N_MESSAGES <= 0) {
        /* Call FileSystem service */
        val jsonRequest = new JSONRPC2Request(RETRIEVE_DIRECTORY_CONTENT_METHOD.name, UUID.randomUUID().toString())

        val namedParams = new HashMap[String, Object]()
        namedParams.put(RETRIEVE_DIRECTORY_CONTENT_METHOD.LABEL_PATH_PARAM, FAKE_MOUNT_POINT)
        namedParams.put(RETRIEVE_DIRECTORY_CONTENT_METHOD.INCLUDE_FILES_PARAM, java.lang.Boolean.TRUE)
        namedParams.put(RETRIEVE_DIRECTORY_CONTENT_METHOD.INCLUDE_DIRS_PARAM, java.lang.Boolean.TRUE)
        jsonRequest.setNamedParams(namedParams)

        val message = session.createTextMessage(jsonRequest.toJSONString())

        /* ReplyTo = Temporary Destination Queue for Server -> Client response */
        message.setJMSReplyTo(replyTmpQueue)

        message.setStringProperty(PROLINE_SERVICE_NAME_KEY, FileSystemService.serviceName)

        messageProducer.send(message)

        logger.info(s"Message [${message.getJMSMessageID()}] sent")
      } else {

        for (i <- 1 to N_MESSAGES) {
          val jsonRpcRequest = new JSONRPC2Request("get_info", Integer.valueOf(i))

          val message = session.createTextMessage(jsonRpcRequest.toJSONString())

          /* ReplyTo = Temporary Destination Queue for Server -> Client response */
          message.setJMSReplyTo(replyTmpQueue)

          /* For test purpose : change some properties on each message ... */
          if ((i % 2) == 0) {
            message.setStringProperty(PROLINE_SERVICE_NAME_KEY, SINGLE_THREADED_INFO_SERVICE_NAME)
          } else {
            message.setStringProperty(PROLINE_SERVICE_NAME_KEY, INFO_SERVICE_NAME)
          }

          if ((i % 3) == 0) {
            message.setStringProperty(PROLINE_SERVICE_VERSION_KEY, SERVICE_VERSION)
          }

          // Step 8. Send the Message
          messageProducer.send(message)

          logger.info(s"Message [${message.getJMSMessageID()}] sent")
        }

      }

      // Producer is closed by session.close() in finally block

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
