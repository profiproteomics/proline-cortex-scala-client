package fr.proline.cortex.client.sample

import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.IOException
import java.util.Date
import java.util.HashMap
import java.util.Map
import javax.jms._

import org.hornetq.api.core.TransportConfiguration
import org.hornetq.api.jms.HornetQJMSClient
import org.hornetq.api.jms.JMSFactoryType
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory
import org.hornetq.core.remoting.impl.netty.TransportConstants
import com.thetransactioncompany.jsonrpc2.JSONRPC2Request
import com.typesafe.scalalogging.StrictLogging

import fr.proline.cortex.api.CortexConstants._
import fr.proline.cortex.client._
import fr.proline.jms.util.JMSConstants._

/* Sample Consumer Clientc code to retrieve a large resource via HornetQ JMS streams 
 * WARN PROLINE_NODE_ID and FILE_PATH must be fixed */
object ResourceClient extends StrictLogging {

  val NettyConnectorFactoryClassName = classOf[NettyConnectorFactory].getName()

  private val RESOURCE_SERVICE_NAME = "proline/misc/ResourceService"

  private val GET_RESOURCE_AS_STREAM_METHOD = "get_resource_as_stream"
  private val FILE_PATH_PARAM_KEY = "file_path"

  // FIXME Modify this ! => if NODE_ID is invalid JMS Message stay in Queue indefinitely
  private val PROLINE_NODE_ID = "77d3e285-c46c-444a-a1d9-5a007c04e8b6"

  private val FILE_PATH = "D:\\Save.zip\\odbg201.zip" // FIXME Modify this !

  private val JMS_HORNET_Q_SAVE_STREAM_KEY = "JMS_HQ_SaveStream"

  // JMS_HQ_SaveStream Blocking
  // bufferedOutput Do not block

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
      val producer = session.createProducer(queue)

      val replyQueue = session.createTemporaryQueue()

      val responseConsumer = session.createConsumer(replyQueue)
      responseConsumer.setMessageListener(ResourceMessageListener)

      connection.start() // Explicitely start connection to begin Consumer reception

      // Step 7. Create a Text Message
      val jsonRequest = new JSONRPC2Request(GET_RESOURCE_AS_STREAM_METHOD,
        Integer.valueOf(1))

      val namedParams = new HashMap[String, AnyRef]()
      namedParams.put(FILE_PATH_PARAM_KEY, FILE_PATH)

      jsonRequest.setNamedParams(namedParams)

      val message = session.createTextMessage(jsonRequest.toJSONString())

      /* ReplyTo = Temporary Destination Queue for Server -> Client response */
      message.setJMSReplyTo(replyQueue)

      /* Set JMS Message Properties */
      message.setStringProperty(PROLINE_SERVICE_NAME_KEY, RESOURCE_SERVICE_NAME)
      message.setStringProperty(PROLINE_NODE_ID_KEY, PROLINE_NODE_ID)

      // Step 8. Send the Message
      producer.send(message)

      logger.info("Message [{}] sent", message.getJMSMessageID())

      // Producer is closed by session.close() in finally block

      ConsoleUtils.waitUserInput() // Streaming LARGE files can be long
      System.out.println("Message reception stopped by user input")
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

    System.out.println("\nMain terminated !")
  }

  private object ResourceMessageListener extends MessageListener {

    def onMessage(jmsMessage: Message) {

      System.out.println("Receiving message n° "
        + BasicMessageListener.MESSAGE_COUNT_SEQUENCE.incrementAndGet() + " : "
        + JMSUtils.formatJmsMessage(jmsMessage))

      if (jmsMessage.isInstanceOf[BytesMessage]) {
        /* It is a Large Message Stream */

        /* Generate a client (local) file name */
        val now = new Date()
        val clientFileName = String.format("downloaded_%tY%<tm%<td%<tH%<tM%<tS_%<tL.dat", now)

        var success = false
        var bos: BufferedOutputStream = null

        try {
          bos = new BufferedOutputStream(new FileOutputStream(clientFileName))

          logger.debug("Saving stream to File [" + clientFileName + ']')

          /* Block until all BytesMessage content is streamed into File OutputStream */
          jmsMessage.setObjectProperty(JMS_HORNET_Q_SAVE_STREAM_KEY, bos)

          success = true
        } catch {
          case ex: Exception => logger.error("Error handling JMS_HQ_SaveStream OutputStream [" + clientFileName + ']', ex)
        } finally {

          if (bos != null) {
            try {
              bos.close()

              if (success) {
                logger.info("File [" + clientFileName + "] Saved")
              }

            } catch {
              case ioEx: IOException => logger.error("Error closing OutputStream [" + clientFileName + ']', ioEx)
            }
          }

        }

      } else if (jmsMessage.isInstanceOf[TextMessage]) {
        /* It is a JSON-RPC Response (Error) */
        val textMessage = jmsMessage.asInstanceOf[TextMessage]

        try {
          val jsonString = textMessage.getText()

          BasicMessageListener.traceJSONResponse(jsonString)
        } catch {
          case ex: Exception => logger.error("Error handling JMS Message", ex)

        }
      } else {
        logger.warn("Invalid JMS Message type")
      }

    }

  }

}
