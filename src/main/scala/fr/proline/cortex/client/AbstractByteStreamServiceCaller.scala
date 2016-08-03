package fr.proline.cortex.client

import java.nio.ByteBuffer

import javax.jms.BytesMessage
import javax.jms.Message
import javax.jms.TextMessage

import scala.collection.mutable.HashMap

import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable
import rx.lang.scala.Subscriber

abstract class AbstractByteStreamServiceCaller extends AbstractJSONRPC2ServiceCaller {
  
  private val BUFFER_SIZE = 1024
  
  /**
   * Execute the remote JSON-RPC method and wait for the response.
   * @param methodName The name of the method to execute remotely.
   * @param namedParameters A Map of named parameters that will be sent in the request.
   * @param convertResponseResult A function used to convert the response from AnyRef to generic T type.
   */
  def executeRemoteMethod(
    methodName: String,
    namedParams: Option[HashMap[String,Any]],
    annotateRequest: Option[TextMessage => Unit]
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[ByteBuffer] = {
    super.executeRemoteMethod[ByteBuffer](methodName, namedParams, annotateRequest, _.asInstanceOf[ByteBuffer] )
  }
  
  /**
   * Method used to parse and emit the remote service response.
   * @param jmsMessage The JMS message to parse.
   */
  override protected def emitRemoteServiceResponse(jmsMessage: Message, responseSuscriber: Subscriber[AnyRef]): Unit = {
    
    val jmsMessageID = jmsMessage.getJMSMessageID
    
    jmsMessage match {
      
      case bytesMessage: BytesMessage => {

        val bytes = new Array[Byte](BUFFER_SIZE)
        val bodyLength = bytesMessage.getBodyLength()
        
        var i = 0
        while (i < bodyLength) {
          val readBytesCount = bytesMessage.readBytes(bytes, BUFFER_SIZE)
          val byteBuffer = ByteBuffer.wrap(bytes, 0, readBytesCount)
           
          // Emit the bytes
          // TODO: the observable is bound to a single thread executor
          // If the stream is big it may block the tread for a long duration
          responseSuscriber.onNext(byteBuffer)
           
          i += BUFFER_SIZE
        }
        
        // Clear the message body
        bytesMessage.clearBody()
      }
      case textMessage: TextMessage => throw newServiceException(
        s"invalid JMS Message($jmsMessageID), it must be a BytesMessage and not a TextMessage (${textMessage.getText}) !"
      )
      case _ => throw newServiceException(
        s"invalid JMS Message($jmsMessageID), it must be a BytesMessage !"
      )
    }
    
  }
  
}
