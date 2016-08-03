package fr.proline.cortex.client.service.misc

import scala.collection.mutable.HashMap

import fr.profi.util.serialization.ProfiJson
import fr.profi.util.primitives._
import fr.proline.cortex.api.fs._
import fr.proline.cortex.api.service.misc.IFileSystemService
import fr.proline.cortex.client.AbstractJSONRPC2ServiceCaller
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

object FileSystem extends AbstractJSONRPC2ServiceCaller with IFileSystemService {
  
  def retrieveAllDirectoryTypes()(implicit callerCtx: RemoteServiceCallerContext): Observable[Array[String]] = {
    this.executeRemoteMethod(RETRIEVE_ALL_DIRECTORY_TYPES_METHOD.name, None, None, convertResponseToStrings)
  }
  
  // Stolen from ExportResultSummaries service => TODO: create an utility for this kind of parsing
  protected def convertResponseToStrings(responseResult: AnyRef): Array[String] = {

    responseResult match {
      case javaArrayList: java.util.ArrayList[_] => {
        require( javaArrayList.isEmpty() == false, mkServiceErrorMsg("the returned List is empty !") )
        
        ProfiJson.getObjectMapper.convertValue(javaArrayList,classOf[Array[String]])
      }
      case _ => throw new Exception( mkServiceErrorMsg("invalid result, list of parameters expected !") )
    }
    
  }
  
  def retrieveAllMountPoints()(implicit callerCtx: RemoteServiceCallerContext): Observable[Array[MountPoint]] = {
    this.executeRemoteMethod(RETRIEVE_ALL_MOUNT_POINTS_METHOD.name, None, None, convertResponseToMountPoints )
  }
  
  def retrieveMountPointsByType(directoryType: String)(implicit callerCtx: RemoteServiceCallerContext): Observable[Array[MountPoint]] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(RETRIEVE_MOUNT_POINTS_BY_TYPE_METHOD.DIR_TYPE_PARAM, directoryType)
    
    this.executeRemoteMethod(RETRIEVE_MOUNT_POINTS_BY_TYPE_METHOD.name, Some(namedParams), None, convertResponseToMountPoints )
  }
  
  def retrieveMountPointsByLabel(label: String)(implicit callerCtx: RemoteServiceCallerContext): Observable[Array[MountPoint]] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(RETRIEVE_MOUNT_POINTS_BY_LABEL_METHOD.LABEL_PARAM, label)
    
    this.executeRemoteMethod(RETRIEVE_MOUNT_POINTS_BY_LABEL_METHOD.name, Some(namedParams), None, convertResponseToMountPoints )
  }
  
  protected def convertResponseToMountPoints(responseResult: AnyRef): Array[MountPoint] = {

    responseResult match {
      case javaArrayList: java.util.ArrayList[_] => {
        require( javaArrayList.isEmpty() == false, mkServiceErrorMsg("the returned list is empty !") )
        
        ProfiJson.getObjectMapper.convertValue(javaArrayList,classOf[Array[MountPoint]])
      }
      case _ => throw new Exception( mkServiceErrorMsg("invalid result, list of parameters expected !") )
    }
    
  }
  
  def retrieveDirectoryContent(
    labelPath: String,
    directoryType: Option[String],
    includeFiles: Long,
    includeDirs: Long,
    extensionFilter: Option[String]
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[Array[FileAttrs]] = {
    
    import RETRIEVE_DIRECTORY_CONTENT_METHOD._
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(LABEL_PATH_PARAM, labelPath)
    if (directoryType.isDefined) namedParams.put(DIRECTORY_TYPE_PARAM, directoryType.get)
    namedParams.put(INCLUDE_FILES_PARAM, includeFiles)
    namedParams.put(INCLUDE_DIRS_PARAM, includeDirs)
    if (extensionFilter.isDefined) namedParams.put(EXTENSION_FILTER_PARAM, extensionFilter)
    
    this.executeRemoteMethod(RETRIEVE_DIRECTORY_CONTENT_METHOD.name, Some(namedParams), None, convertResponseToFileAttrsArray)
  }
  
  protected def convertResponseToFileAttrsArray(responseResult: AnyRef): Array[FileAttrs] = {

    responseResult match {
      case javaArrayList: java.util.ArrayList[_] => {
        require( javaArrayList.isEmpty() == false, mkServiceErrorMsg("the returned list is empty !") )
        
        ProfiJson.getObjectMapper.convertValue(javaArrayList,classOf[Array[FileAttrs]])
      }
      case _ => throw new Exception( mkServiceErrorMsg("invalid result, list of parameters expected !") )
    }
    
  }
  
}