package fr.proline.cortex.client

import java.util.Map
import javax.jms._
import com.thetransactioncompany.jsonrpc2._
import com.typesafe.scalalogging.StrictLogging
import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.JMSNotificationMessage

class ServiceNotificationListener(
  var callback: Option[(JMSNotificationMessage) => Unit] = None
) extends MessageListener with StrictLogging {

  override def onMessage(jmsMessage: Message) {
    logger.debug(s"Notification Listener receiving message ${jmsMessage.getJMSMessageID }")
    logger.trace(s"Content of message ${jmsMessage.getJMSMessageID} is:" + JMSUtils.formatJmsMessage(jmsMessage) )

    jmsMessage match {
      case textMessage: TextMessage => {
        if (callback.isEmpty) return ()

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
