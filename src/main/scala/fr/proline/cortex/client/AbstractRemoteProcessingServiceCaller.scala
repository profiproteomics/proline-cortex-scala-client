package fr.proline.cortex.client

import scala.collection.mutable.HashMap

import fr.proline.cortex.api.CortexConstants
import fr.proline.cortex.caller._
import fr.proline.cortex.client.context._

import rx.lang.scala.Observable

abstract class AbstractRemoteProcessingServiceCaller[T] extends AbstractJSONRPC2ServiceCaller {
  
  /**
   * Abstract method used to convert the remote service response result.
   * @param responseResult The AnyRef result which will be converted to the generic type T.
   */
  protected def convertResponseResult(responseResult: AnyRef): T
  
  /**
   * Execute the remote JSON-RPC processing service and wait for the response.
   * @param namedParameters A Map of named parameters that will be sent in the request.
   */
  def runService(
    namedParams: Option[HashMap[String,Any]] = None
  )(implicit callerCtx: JMSServiceCallerContext): JMSServiceCall[T] = {
    executeRemoteMethod(CortexConstants.PROLINE_PROCESS_METHOD_NAME, namedParams, None, convertResponseResult)
  }
  
}