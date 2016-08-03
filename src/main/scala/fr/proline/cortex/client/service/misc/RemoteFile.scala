package fr.proline.cortex.client.service.misc

import java.io.BufferedOutputStream

import java.io.File
import java.io.FileOutputStream

import javax.jms.TextMessage

import scala.collection.mutable.HashMap

import org.hornetq.api.jms.HornetQJMSConstants

import fr.proline.cortex.api.CortexConstants
import fr.proline.cortex.api.service.misc._
import fr.proline.cortex.client.AbstractBinaryDataServiceCaller
import fr.proline.cortex.client.context.RemoteServiceCallerContext
import fr.proline.jms.util.JMSConstants

import rx.lang.scala.Observable

object RemoteFile extends AbstractBinaryDataServiceCaller with IMiscService {
  
  val serviceLabel = "ResourceService"
  val serviceVersion = "1.0"
  val methodDefinitions = Seq()
  
  def download(remoteFilePath: String, localFilePath: String, serverNodeId: String)(implicit callerCtx: RemoteServiceCallerContext): Observable[File] = {
    
    // TODO: create the corresponding PARAM API
    val namedParams = new HashMap[String,Any]()
    namedParams.put("file_path", remoteFilePath)
    
    val requestAnnotator = { textMessage: TextMessage =>
      textMessage.setStringProperty(JMSConstants.PROLINE_NODE_ID_KEY, serverNodeId)
      ()
    }
    
    this.executeRemoteMethod(CortexConstants.PROLINE_GET_RESOURCE_AS_STREAM_METHOD_NAME, Some(namedParams), Some(requestAnnotator) ).map { bytesMessage =>
      val outputFile = new File(localFilePath)
      val fileOutputStream = new FileOutputStream(outputFile)
      val bufferedOutput = new BufferedOutputStream(fileOutputStream)
                      
      // This will block until the entire content is saved on disk
      bytesMessage.setObjectProperty(HornetQJMSConstants.JMS_HORNETQ_SAVE_STREAM, bufferedOutput)
      
      // This won't wait the stream to finish. We need to keep the consumer active.
      //bytesMessage.setObjectProperty("JMS_HQ_OutputStream", bufferedOutput)
      
      outputFile
    }
  }
  
}