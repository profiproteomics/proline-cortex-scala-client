package fr.proline.cortex.client.service.dps.msi

import java.io.File

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractJSONRPC2ServiceCaller
import fr.proline.cortex.client.RemoteServiceCall
import fr.proline.cortex.client.context.RemoteServiceCallerContext
import fr.proline.cortex.client.service.misc.RemoteFile
import fr.proline.jms.util.JMSConstants

import rx.lang.scala.Observable

object ExportResultSummaries extends AbstractJSONRPC2ServiceCaller with IExportResultSummariesServiceV2_0 {
  
  import this.PROCESS_METHOD._
  
  private val requiredKeys = Array("file_paths", JMSConstants.PROLINE_NODE_ID_KEY)
  
  def export(
    rsmIdentifiers: Seq[RsmIdentifier],
    fileFormat: FileFormat.Value,
    fileName: Option[String] = None,
    fileDirectory: Option[String] = None,
    extraParams: Option[Map[String, Any]] = None
  )(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceCall[Array[String]] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(RSM_IDENTIFIERS_PARAM, rsmIdentifiers)
    namedParams.put(FILE_FORMAT_PARAM, fileFormat.toString )
    
    if (fileName.isDefined) namedParams.put(FILE_NAME_PARAM, fileName.get)
    if (fileDirectory.isDefined) namedParams.put(FILE_DIRECTORY_PARAM, fileDirectory.get)
    namedParams.put(OUTPUT_MODE_PARAM, OutputMode.FILE.toString)
    if (extraParams.isDefined) namedParams.put(EXTRA_PARAMS_PARAM, extraParams.get)
    
    this.executeRemoteMethod(PROCESS_METHOD.name, Some(namedParams), None, convertFileModeResult )
  }
  
  protected def convertFileModeResult(responseResult: AnyRef): Array[String] = {

    responseResult match {
      case javaArrayList: java.util.ArrayList[_] => {
        require( javaArrayList.isEmpty() == false, mkServiceErrorMsg("the returned List is empty !") )
        
        ProfiJson.getObjectMapper.convertValue(javaArrayList,classOf[Array[String]])
      }
      case _ => throw new Exception( mkServiceErrorMsg("invalid result, list of parameters expected !") )
    }
    
  }
  
  def exportThenDownload(
    rsmIdentifiers: Seq[RsmIdentifier],
    fileFormat: FileFormat.Value,
    extraParams: Option[Map[String, Any]],
    localFileDirectory: String
  )(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceCall[File] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(RSM_IDENTIFIERS_PARAM, rsmIdentifiers)
    namedParams.put(FILE_FORMAT_PARAM, fileFormat.toString)
    namedParams.put(OUTPUT_MODE_PARAM, OutputMode.STREAM.toString)
    if (extraParams.isDefined) namedParams.put(EXTRA_PARAMS_PARAM, extraParams.get)
    
    val serviceCall = this.executeRemoteMethod(PROCESS_METHOD.name, Some(namedParams), None, convertStreamModeResult )
    
    val newObservable = serviceCall.result.flatMap { resultMap =>
      val prolineNodeId = resultMap(JMSConstants.PROLINE_NODE_ID_KEY).asInstanceOf[String]
      val filePaths = resultMap("file_paths").asInstanceOf[List[String]]
      
      val observableFiles = filePaths.map { filePath =>
        val localFilePath = localFileDirectory + '/' + new File(filePath).getName
        
        RemoteFile.download(filePath, localFilePath, prolineNodeId).result
        //RemoteResource.downloadToFile(filePath, localFilePath, prolineNodeId)
      }
      
      Observable.from(observableFiles).concat
    }
    
    serviceCall.newResult(newObservable)
  }
  
  // Stolen from Database service => TODO: create an utility for this kind of parsing
  protected def convertStreamModeResult(responseResult: AnyRef): Map[String, Object] = {

    responseResult match {
      case javaMap: java.util.Map[_, _] => {
        require( javaMap.isEmpty() == false, mkServiceErrorMsg("the returned Map is empty !") )
        
        val scalaMap = ProfiJson.getObjectMapper.convertValue(javaMap,classOf[Map[String, Object]])
        
        for (requiredKey <- requiredKeys) {
          val valueOpt = scalaMap.get(requiredKey)
          assert(
            valueOpt.isDefined && valueOpt.get != null, mkServiceErrorMsg(s"key '$requiredKey' is missing in returned Map !")
          )
        }
        
        scalaMap
      }
      case _ => throw new Exception( mkServiceErrorMsg("invalid result, map of parameters expected !") )
    }
    
  }
  
}