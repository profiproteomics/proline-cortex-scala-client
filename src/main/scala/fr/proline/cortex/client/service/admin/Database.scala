package fr.proline.cortex.client.service.admin

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.admin.IGetConnectionTemplateService
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.RemoteServiceExchange
import fr.proline.cortex.client.context.RemoteServiceCallerContext
import fr.proline.repository.AbstractDatabaseConnector

import rx.lang.scala.Observable

object Database extends AbstractRemoteProcessingServiceCaller[Map[String, Object]] with IGetConnectionTemplateService {
  
  private val requiredKeys = Array(
    AbstractDatabaseConnector.PERSISTENCE_JDBC_DRIVER_KEY,
    AbstractDatabaseConnector.PERSISTENCE_JDBC_URL_KEY,
    AbstractDatabaseConnector.PERSISTENCE_JDBC_USER_KEY
  )
  
  def getConnectionTemplate()(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceExchange[Map[String, Object]] = {
    this.runService()
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Map[String, Object] = {

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
      case _ => throw new Exception( mkServiceErrorMsg("invalid result, dabasase Parameters not returned !") )
    }
    
  }
  
  // Expose the method defined in the UserAccount service
  // TODO: define the same API server side or add login/passwordHash params to the getConnectionTemplate method
  def getEncryptedPassword(
    login: String,
    passwordHash: String,
    publicKey: String
  )(implicit callerCtx: RemoteServiceCallerContext): RemoteServiceExchange[String] = {
    UserAccount.getDatabasePassword(login, passwordHash, publicKey)
  }
    
}