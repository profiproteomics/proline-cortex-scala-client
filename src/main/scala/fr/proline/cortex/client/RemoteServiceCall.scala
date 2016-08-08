package fr.proline.cortex.client

import javax.jms.Message
import javax.jms.MessageHeader.Header

import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

object RemoteServiceCall {
  
  def messageToPropertyMap(message: Message): Map[String, Any] = {
    
    val requestPropertyByName = new HashMap[String,Any]()
    val properties = message.getPropertyNames()
    
    // Add header
    requestPropertyByName.put(Header.JMSCorrelationID.toString, message.getJMSCorrelationID)
    requestPropertyByName.put(Header.JMSCorrelationIDAsBytes.toString, message.getJMSCorrelationIDAsBytes)
    requestPropertyByName.put(Header.JMSDeliveryMode.toString, message.getJMSDeliveryMode)
    requestPropertyByName.put(Header.JMSDeliveryTime.toString, message.getJMSDeliveryTime)
    requestPropertyByName.put(Header.JMSDestination.toString, message.getJMSDestination)
    requestPropertyByName.put(Header.JMSExpiration.toString, message.getJMSExpiration)
    requestPropertyByName.put(Header.JMSMessageID.toString, message.getJMSMessageID)
    requestPropertyByName.put(Header.JMSPriority.toString, message.getJMSPriority)
    requestPropertyByName.put(Header.JMSRedelivered.toString, message.getJMSRedelivered)
    requestPropertyByName.put(Header.JMSReplyTo.toString, message.getJMSReplyTo)
    requestPropertyByName.put(Header.JMSTimestamp.toString, message.getJMSTimestamp)
    requestPropertyByName.put(Header.JMSType.toString, message.getJMSType)
    
    // Add properties
    while (properties.hasMoreElements()) {
      val propertyName = properties.nextElement().asInstanceOf[String]
      requestPropertyByName.put(propertyName, message.getObjectProperty(propertyName) )
    }
    
    requestPropertyByName.toMap
  }
  
}

class RemoteServiceCall[+T](
  val request: Future[Message],
  val result: Future[T]
) {
  
  def map[S](f: T => S)(implicit ec: ExecutionContext): RemoteServiceCall[S] = newResult(mapResult(f)(ec))
  
  def mapResult[S](f: T => S)(implicit ec: ExecutionContext): Future[S] = result.map { f }
  
  def newResult[S](newResult: Future[S]): RemoteServiceCall[S] = {
    new RemoteServiceCall(request, newResult)
  }
}