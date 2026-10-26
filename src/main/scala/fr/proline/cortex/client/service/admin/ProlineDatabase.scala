package fr.proline.cortex.client.service.admin

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.admin.IGetConnectionTemplateService
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context._
import fr.proline.repository.AbstractDatabaseConnector

import rx.lang.scala.Observable

trait IProlineDatabaseServiceCaller extends IServiceCaller {
  def getConnectionTemplate(): IServiceCall[collection.Map[String, Object]]
  def getEncryptedPassword(
    login: String,
    passwordHash: String,
    publicKey: String
  ): IServiceCall[String]
}

case class ProlineDatabase()(implicit val callerCtx: JMSServiceCallerContext) extends AbstractRemoteProcessingServiceCaller[Map[String, Object]]
  with IGetConnectionTemplateService
  with IProlineDatabaseServiceCaller {
  
  private val requiredKeys = Array(
    AbstractDatabaseConnector.PERSISTENCE_JDBC_DRIVER_KEY,
    AbstractDatabaseConnector.PERSISTENCE_JDBC_URL_KEY,
    AbstractDatabaseConnector.PERSISTENCE_JDBC_USER_KEY
  )
  
  def getConnectionTemplate(): JMSServiceCall[Map[String, Object]] = {
    this.runService()
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Map[String, Object] = {

    responseResult match {
      case javaMap: java.util.Map[_, _] => {
        require( javaMap.isEmpty() == false, mkServiceErrorMsg("the returned Map is empty !") )
        
        val scalaMap = ProfiJson.getObjectMapper().convertValue(javaMap,classOf[Map[String, Object]])
        
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
  ): JMSServiceCall[String] = {
    ProlineUserAccount().getDatabasePassword(login, passwordHash, publicKey)
  }
    
}