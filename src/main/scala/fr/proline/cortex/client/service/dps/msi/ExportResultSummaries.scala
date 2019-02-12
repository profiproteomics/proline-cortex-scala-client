package fr.proline.cortex.client.service.dps.msi

import java.io.File

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._
import fr.proline.cortex.client.service.misc.RemoteFile
import fr.proline.jms.util.JMSConstants

trait IExportResultSummariesServiceCaller extends IServiceCaller {
  def export(
    rsmIdentifiers: Seq[ExportResultSummaryIdentifier],
    fileFormat: ExportFileFormat.Value,
    fileDirectory: String,
    fileName: String,
    extraParams: Option[Map[String, Any]] = None
  ): IServiceCall[Array[String]]
}

case class ExportResultSummaries()(implicit val callerCtx: JMSServiceCallerContext)
    extends AbstractJSONRPC2ServiceCaller
    with IExportResultSummariesServiceV2_0
    with IExportResultSummariesServiceCaller {
  
  import this.PROCESS_METHOD._
  
  private val requiredKeys = Array("file_paths", JMSConstants.PROLINE_NODE_ID_KEY)
  
  def export(
    rsmIdentifiers: Seq[ExportResultSummaryIdentifier],
    fileFormat: ExportFileFormat.Value,
    fileDirectory: String,
    fileName: String,
    extraParams: Option[Map[String, Any]] = None
  ): JMSServiceCall[Array[String]] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(RSM_IDENTIFIERS_PARAM, rsmIdentifiers)
    namedParams.put(FILE_FORMAT_PARAM, fileFormat.toString )
    
    namedParams.put(FILE_DIRECTORY_PARAM, fileDirectory)
    namedParams.put(FILE_NAME_PARAM, fileName)
    namedParams.put(OUTPUT_MODE_PARAM, ExportOutputMode.FILE.toString)
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
    rsmIdentifiers: Seq[ExportResultSummaryIdentifier],
    fileFormat: ExportFileFormat.Value,
    extraParams: Option[Map[String, Any]],
    localFileDirectory: String
  )(implicit ec: ExecutionContext): JMSServiceCall[List[File]] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(RSM_IDENTIFIERS_PARAM, rsmIdentifiers)
    namedParams.put(FILE_FORMAT_PARAM, fileFormat.toString)
    namedParams.put(OUTPUT_MODE_PARAM, ExportOutputMode.STREAM.toString)
    if (extraParams.isDefined) namedParams.put(EXTRA_PARAMS_PARAM, extraParams.get)
    
    val serviceCall = this.executeRemoteMethod(PROCESS_METHOD.name, Some(namedParams), None, convertStreamModeResult )
    
    val newFuture = serviceCall.result.flatMap { resultMap =>
      val prolineNodeId = resultMap(JMSConstants.PROLINE_NODE_ID_KEY).asInstanceOf[String]
      val filePaths = resultMap("file_paths").asInstanceOf[List[String]]
      
      val futureFiles = filePaths.map { filePath =>
        val localFilePath = localFileDirectory + '/' + new File(filePath).getName
        
        RemoteFile().download(filePath, localFilePath, prolineNodeId).result
        //RemoteResource.downloadToFile(filePath, localFilePath, prolineNodeId)
      }
      
      Future.sequence(futureFiles)
    }
    
    serviceCall.newResult(newFuture)
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