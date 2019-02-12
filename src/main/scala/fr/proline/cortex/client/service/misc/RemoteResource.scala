package fr.proline.cortex.client.service.misc

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files

import javax.jms.Message
import javax.jms.TextMessage

import scala.collection.mutable.HashMap
import scala.concurrent.Future

import fr.profi.util.concurrent.ObservableUtils
import fr.proline.cortex.api.CortexConstants
import fr.proline.cortex.api.service.misc._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._
import fr.proline.jms.util.JMSConstants

import rx.lang.scala.Observable

case class RemoteResource()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractByteStreamServiceCaller
  with IMiscService {
  
  val serviceLabel = "ResourceService"
  val serviceVersion = "1.0"
  val methodDefinitions = Seq()
  
  def getBytes(
    remoteFilePath: String,
    serverNodeId: String
  ): (Future[Message],Observable[ByteBuffer]) = {
    
    // TODO: create the corresponding PARAM API
    val namedParams = new HashMap[String,Any]()
    namedParams.put("file_path", remoteFilePath)
    
    val messageAnnotator = { textMessage: TextMessage =>
      textMessage.setStringProperty(JMSConstants.PROLINE_NODE_ID_KEY, serverNodeId)
      ()
    }
    
    this.observeRemoteMethodResult(CortexConstants.PROLINE_GET_RESOURCE_AS_STREAM_METHOD_NAME, Some(namedParams), Some(messageAnnotator))
  }
  
  def downloadToFile(
    remoteFilePath: String,
    localFilePath: String,
    serverNodeId: String
  ): JMSServiceCall[File] = {
    
    val file = new File(localFilePath)
    
    val options = new java.util.HashSet[java.nio.file.OpenOption]()
    options.add(java.nio.file.StandardOpenOption.APPEND)
    options.add(java.nio.file.StandardOpenOption.CREATE)
    val seekableByteChannel = Files.newByteChannel(file.toPath(), options)
    
    val (bytesMessageRequest,bytesMessageResult) = this.getBytes(remoteFilePath, serverNodeId)

    val observableFile = Observable[File] { suscriber =>
      
      bytesMessageResult.doAfterTerminate { () =>
        seekableByteChannel.close()
      }.subscribe(
        onNext = { byteBuffer: ByteBuffer => 
          seekableByteChannel.write(byteBuffer)
        },
        onError = { error: Throwable =>
          suscriber.onError(error)
        },
        onCompleted = { () =>  
          suscriber.onNext(file)
          suscriber.onCompleted()
        }
      )
      
    }
    
    new JMSServiceCall( bytesMessageRequest, ObservableUtils.observableToFuture(observableFile) )
  }
  
}