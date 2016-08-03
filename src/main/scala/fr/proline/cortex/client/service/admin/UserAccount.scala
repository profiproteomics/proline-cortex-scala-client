package fr.proline.cortex.client.service.admin

import scala.collection.mutable.HashMap

import fr.profi.util.primitives._
import fr.proline.cortex.api.service.admin.IUserAccountService
import fr.proline.cortex.client.AbstractJSONRPC2ServiceCaller
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

object UserAccount extends AbstractJSONRPC2ServiceCaller with IUserAccountService {
  
  def create(login: String, passwordHash: String)(implicit callerCtx: RemoteServiceCallerContext): Observable[Long] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(CREATE_METHOD.LOGIN_PARAM, login)
    namedParams.put(CREATE_METHOD.PASSWORD_HASH_PARAM, passwordHash )
    
    this.executeRemoteMethod(CREATE_METHOD.name, Some(namedParams), None, toLong )
  }
  
  def changePassword(login: String, oldPasswordHash: String, newPasswordHash: String)(implicit callerCtx: RemoteServiceCallerContext): Observable[Boolean] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(CHANGE_PASSWORD_METHOD.LOGIN_PARAM, login)
    namedParams.put(CHANGE_PASSWORD_METHOD.OLD_PASSWORD_HASH_PARAM, oldPasswordHash )
    namedParams.put(CHANGE_PASSWORD_METHOD.NEW_PASSWORD_HASH_PARAM, newPasswordHash )
    
    this.executeRemoteMethod(CHANGE_PASSWORD_METHOD.name, Some(namedParams), None, toBoolean )
  }
  
  def authenticate(
    login: String,
    passwordHash: String
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[Boolean] = {
    
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
  )(implicit callerCtx: RemoteServiceCallerContext): Observable[String] = {
    
    val namedParams = new HashMap[String,Any]()
    namedParams.put(AUTHENTICATE_METHOD.LOGIN_PARAM, login)
    namedParams.put(AUTHENTICATE_METHOD.PASSWORD_HASH_PARAM, passwordHash )
    namedParams.put(AUTHENTICATE_METHOD.RETURN_DB_PASSWORD_PARAM, true )
    namedParams.put(AUTHENTICATE_METHOD.PUBLIC_KEY_PARAM, publicKey )
    
    this.executeRemoteMethod(AUTHENTICATE_METHOD.name, Some(namedParams), None, _.asInstanceOf[String] )
  }
  
}