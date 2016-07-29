package fr.proline.jms.util

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Enumeration
import javax.jms.Message
import javax.jms.TextMessage
import com.typesafe.scalalogging.LazyLogging
import fr.profi.util.StringUtils.LINE_SEPARATOR
import fr.proline.jms.util.JMSConstants._

object JMSUtils extends LazyLogging {

  private val DATE_FORMAT_AS_STR = "yyyy.MM.dd G 'at' HH:mm:ss" // "%td/%<tm/%<tY %<tH:%<tM:%<tS.%<tL"
  
  val DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_AS_STR)

  private val MESSAGE_BUFFER_SIZE = 2048

  private val NULL_STR = "NULL"
  
  private val TAB = "    "

  /**
   * Formats some Header fields, Properties and Body of the given JMS Message to print useful debug info.
   */
  def formatJmsMessage(jmsMessage: Message): String = {

    require(jmsMessage != null, "message is null")

    val buff = new StringBuilder(MESSAGE_BUFFER_SIZE)

    try {
      buff
        .append(jmsMessage.getClass().getName())
        .append("  ")
        .append(jmsMessage.getJMSMessageID())
        .append(LINE_SEPARATOR)

      _appendLine(buff, JMSCorrelationID, jmsMessage.getJMSCorrelationID)
      _appendLine(buff, JMSTimestamp, DATE_FORMAT.format(jmsMessage.getJMSTimestamp))
      _appendLine(buff, JMSDestination, jmsMessage.getJMSDestination.toString)
      _appendLine(buff, JMSReplyTo, Option(jmsMessage.getJMSReplyTo).getOrElse("NULL").toString)

      val nameEnum = jmsMessage.getPropertyNames().asInstanceOf[Enumeration[String]]

      while (nameEnum.hasMoreElements()) {
        val propertyName = nameEnum.nextElement()
        val propertyValue = jmsMessage.getStringProperty(propertyName)
        
        buff
          .append(TAB)
          .append('[').append(propertyName).append("] : ")
          
        if (propertyValue == null) {
          buff.append(NULL_STR)
        } else {
          buff.append('[').append(propertyValue).append(']')
        }

        buff.append(LINE_SEPARATOR)
      }

      jmsMessage match {
        case tm: TextMessage => buff.append(TAB).append(tm.getText)
      }
      // TODO: do we need this ???
      buff.append(LINE_SEPARATOR)
      
    } catch {
      case ex: Throwable =>
        logger.error("Error retrieving JMS Message header or content", ex)
    }

    buff.toString()
  }
  
  @inline private def _appendLine(sb: StringBuilder, key: String, value: String) {
    sb.append(TAB)
      .append(key)
      .append(' ')
      
    if (value == null) {
      sb.append(NULL_STR)
    } else {
      sb.append(value)
    }
    
    sb.append(LINE_SEPARATOR)
  }

}
