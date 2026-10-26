package fr.proline.cortex.client

import java.util.Map
import java.util.Set
import java.util.concurrent.atomic.AtomicInteger
import javax.jms.Message
import javax.jms.MessageListener
import javax.jms.TextMessage

import scala.jdk.CollectionConverters._

import com.thetransactioncompany.jsonrpc2._
import com.typesafe.scalalogging.StrictLogging

import fr.proline.jms.util.JMSUtils

object BasicMessageListener extends MessageListener with StrictLogging {
  
  /* Count received messages */
  val MESSAGE_COUNT_SEQUENCE = new AtomicInteger(0)



  def onMessage(jmsMessage: Message): Unit = {
    logger.info("Receiving message #" + MESSAGE_COUNT_SEQUENCE.incrementAndGet() + ": " + JMSUtils.formatJmsMessage(jmsMessage))

    jmsMessage match {
      case tm: TextMessage => {
        try {
          val jsonString = tm.getText()

          /* TODO Use JSON-RPC Response */
          traceJSONResponse(jsonString)
        } catch {
          case ex: Exception => logger.error("Error handling JMS Message", ex)
        }
      }
      case _ => logger.warn("Invalid JMS Message type")
    }

  }

  def traceJSONResponse(jsonString: String): Unit = {
    val jsonMessage = JSONRPC2Message.parse(jsonString)

    jsonMessage match {
      case jsonRpcNotification: JSONRPC2Notification => {
        println("JSON Notification method: " + jsonRpcNotification.getMethod())

        val namedParams = jsonRpcNotification.getNamedParams()

        if ((namedParams != null) && !namedParams.isEmpty()) {
          val buff = new StringBuilder("Params: ")

          var first = true

          val entries = namedParams.entrySet().asScala

          for (entry <- entries) {

            if (first) {
              first = false
            } else {
              buff.append(" | ")
            }

            buff.append(entry.getKey())
            buff.append(" : ").append(entry.getValue())
          }

          println(buff.toString)
        }
      }
      case jsonRpcResponse: JSONRPC2Response => {

        println("JSON Response Id: " + jsonRpcResponse.getID())

        val jsonError = jsonRpcResponse.getError()

        if (jsonError != null) {
          val errorMsg = s"JSON Error code ${jsonError.getCode}, message: ${'"'}${jsonError.getMessage}${'"'}"
          logger.error(errorMsg)
          logger.error("JSON Throwable", jsonError)
        }

        val result = jsonRpcResponse.getResult()

        if (result == null) {
          println("No result")
        } else {
          println("Result :\n" + result)
        }
      }
    }

  }

}
