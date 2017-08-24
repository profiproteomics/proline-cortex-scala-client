package fr.proline.cortex.client.service.admin

import javax.jms.Message

import scala.collection.mutable.HashMap

import fr.profi.util.primitives._
import fr.proline.cortex.api.service.admin.IUserAccountService
import fr.proline.cortex.caller._
import fr.proline.cortex.client._

trait IProlineUserAccountServiceCaller {
  def create(login: String, passwordHash: String, isAdmin: Boolean)(implicit callerCtx: IServiceCallerContext): IServiceCall[Long]
  def changePassword(login: String, oldPasswordHash: String, newPasswordHash: String)(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean]
  def changeAdminStatus(userId: Long, isAdmin: Boolean)(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean]
  def authenticate(
    login: String,
    passwordHash: String
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean]
}

object ProlineUserAccount extends AbstractJSONRPC2ServiceCaller
    with IUserAccountService
    with IProlineUserAccountServiceCaller {
  
  def create(
    login: String,
    passwordHash: String,
    isAdmin: Boolean = false
  )(implicit callerCtx: IServiceCallerContext): JMSServiceCall[Long] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(CREATE_METHOD.LOGIN_PARAM, login)
    namedParams.put(CREATE_METHOD.PASSWORD_HASH_PARAM, passwordHash)
    namedParams.put(CREATE_METHOD.IS_USER_GROUP_PARAM, !isAdmin)
    
    this.executeRemoteMethod(CREATE_METHOD.name, Some(namedParams), None, toLong )
  }
  
  def changePassword(login: String, oldPasswordHash: String, newPasswordHash: String)(implicit callerCtx: IServiceCallerContext): JMSServiceCall[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(CHANGE_PASSWORD_METHOD.LOGIN_PARAM, login)
    namedParams.put(CHANGE_PASSWORD_METHOD.OLD_PASSWORD_HASH_PARAM, oldPasswordHash )
    namedParams.put(CHANGE_PASSWORD_METHOD.NEW_PASSWORD_HASH_PARAM, newPasswordHash )
    
    this.executeRemoteMethod(CHANGE_PASSWORD_METHOD.name, Some(namedParams), None, toBoolean )
  }
  
  // FIXME: this is a severe security failure => it should not be exposed as a service
  def changeAdminStatus(userId: Long, isAdmin: Boolean)(implicit callerCtx: IServiceCallerContext): JMSServiceCall[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(MODIFY_USER_GROUP_METHOD.LOGIN_ID_PARAM, userId)
    namedParams.put(MODIFY_USER_GROUP_METHOD.IS_USER_GROUP_PARAM, !isAdmin)
    
    this.executeRemoteMethod(MODIFY_USER_GROUP_METHOD.name, Some(namedParams), None, toBoolean )
  }
  
  def authenticate(
    login: String,
    passwordHash: String
  )(implicit callerCtx: IServiceCallerContext): JMSServiceCall[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(AUTHENTICATE_METHOD.LOGIN_PARAM, login)
    namedParams.put(AUTHENTICATE_METHOD.PASSWORD_HASH_PARAM, passwordHash )
    namedParams.put(AUTHENTICATE_METHOD.RETURN_DB_PASSWORD_PARAM, false )
   
    this.executeRemoteMethod(AUTHENTICATE_METHOD.name, Some(namedParams), None, toBoolean )
  }
  
  // Method exposed in the Database service
  protected[admin] def getDatabasePassword(
    login: String,
    passwordHash: String,
    publicKey: String
  )(implicit callerCtx: IServiceCallerContext): JMSServiceCall[String] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(AUTHENTICATE_METHOD.LOGIN_PARAM, login)
    namedParams.put(AUTHENTICATE_METHOD.PASSWORD_HASH_PARAM, passwordHash )
    namedParams.put(AUTHENTICATE_METHOD.RETURN_DB_PASSWORD_PARAM, true )
    namedParams.put(AUTHENTICATE_METHOD.PUBLIC_KEY_PARAM, publicKey )
    
    this.executeRemoteMethod(AUTHENTICATE_METHOD.name, Some(namedParams), None, _.asInstanceOf[String] )
  }
  
}