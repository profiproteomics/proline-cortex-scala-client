package fr.proline.cortex.client.service.misc

import scala.collection.mutable.HashMap

import fr.profi.util.serialization.ProfiJson
import fr.profi.util.primitives._
import fr.proline.cortex.api.fs._
import fr.proline.cortex.api.service.misc.IFileSystemService
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._

trait IFileSystemServiceCaller extends IServiceCaller {
  def getAllDirectoryTypes(): IServiceCall[Array[String]]
  def getAllMountPoints(): IServiceCall[Array[MountPoint]]
  def findMountPointsByType(directoryType: String): IServiceCall[Array[MountPoint]]
  def findMountPointsByLabel(label: String): IServiceCall[Array[MountPoint]]
  def getDirectoryContent(
    labelPath: String,
    //directoryType: Option[String],
    includeFiles: Boolean,
    includeDirs: Boolean,
    extensionFilter: Option[String]
  ): IServiceCall[Array[_ <: FileOrDirAttrs]]
}

case class FileSystem()(implicit val callerCtx: JMSServiceCallerContext)
  extends AbstractJSONRPC2ServiceCaller
  with IFileSystemService
  with IFileSystemServiceCaller {
  
  def getAllDirectoryTypes(): JMSServiceCall[Array[String]] = {
    this.executeRemoteMethod(RETRIEVE_ALL_DIRECTORY_TYPES_METHOD.name, None, None, convertResponseToStrings)
  }
  
  // Stolen from ExportResultSummaries service => TODO: create an utility for this kind of parsing
  protected def convertResponseToStrings(responseResult: AnyRef): Array[String] = {

    responseResult match {
      case javaArrayList: java.util.ArrayList[_] => {
        require( javaArrayList.isEmpty() == false, mkServiceErrorMsg("the returned List is empty !") )
        
        ProfiJson.getObjectMapper().convertValue(javaArrayList,classOf[Array[String]])
      }
      case _ => throw new Exception( mkServiceErrorMsg("invalid result, list of parameters expected !") )
    }
    
  }
  
  def getAllMountPoints(): JMSServiceCall[Array[MountPoint]] = {
    this.executeRemoteMethod(RETRIEVE_ALL_MOUNT_POINTS_METHOD.name, None, None, convertResponseToMountPoints )
  }
  
  def findMountPointsByType(directoryType: String): JMSServiceCall[Array[MountPoint]] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(RETRIEVE_MOUNT_POINTS_BY_TYPE_METHOD.DIR_TYPE_PARAM, directoryType)
    
    this.executeRemoteMethod(RETRIEVE_MOUNT_POINTS_BY_TYPE_METHOD.name, Some(namedParams), None, convertResponseToMountPoints )
  }
  
  def findMountPointsByLabel(label: String): JMSServiceCall[Array[MountPoint]] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(RETRIEVE_MOUNT_POINTS_BY_LABEL_METHOD.LABEL_PARAM, label)
    
    this.executeRemoteMethod(RETRIEVE_MOUNT_POINTS_BY_LABEL_METHOD.name, Some(namedParams), None, convertResponseToMountPoints )
  }
  
  protected def convertResponseToMountPoints(responseResult: AnyRef): Array[MountPoint] = {

    responseResult match {
      case javaArrayList: java.util.ArrayList[_] => {
        require( javaArrayList.isEmpty() == false, mkServiceErrorMsg("the returned list is empty !") )
        
        ProfiJson.getObjectMapper().convertValue(javaArrayList,classOf[Array[MountPoint]])
      }
      case _ => throw new Exception( mkServiceErrorMsg("invalid result, list of parameters expected !") )
    }
    
  }
  
  def getDirectoryContent(
    labelPath: String,
    //directoryType: Option[String],
    includeFiles: Boolean,
    includeDirs: Boolean,
    extensionFilter: Option[String]
  ): JMSServiceCall[Array[_ <: FileOrDirAttrs]] = {
    
    import RETRIEVE_DIRECTORY_CONTENT_METHOD._
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(LABEL_PATH_PARAM, labelPath)
    //if (directoryType.isDefined) namedParams.put(DIRECTORY_TYPE_PARAM, directoryType.get)
    namedParams.put(INCLUDE_FILES_PARAM, includeFiles)
    namedParams.put(INCLUDE_DIRS_PARAM, includeDirs)
    if (extensionFilter.isDefined) namedParams.put(EXTENSION_FILTER_PARAM, extensionFilter)
    
    this.executeRemoteMethod(RETRIEVE_DIRECTORY_CONTENT_METHOD.name, Some(namedParams), None, convertResponseToFileAttrsArray)
  }
  
  protected def convertResponseToFileAttrsArray(responseResult: AnyRef): Array[_ <: FileOrDirAttrs] = {

    responseResult match {
      case javaArrayList: java.util.ArrayList[_] => {
        ProfiJson.getObjectMapper().convertValue(javaArrayList,classOf[Array[FileAttrs]])
      }
      case _ => throw new Exception( mkServiceErrorMsg("invalid result, list of parameters expected !") )
    }
    
  }
  
}