package fr.proline.cortex.client

import com.thetransactioncompany.jsonrpc2.JSONRPC2Request

import fr.proline.cortex.api.CortexConstants

import rx.lang.scala.Observable

abstract class AbstractRemoteProcessingServiceCaller[T] extends AbstractJSONRPC2ServiceCaller[T] {
  
  /**
   * Execute the remote JSON-RPC processing service and wait for the response.
   * @param namedParameters A Map of named parameters that will be sent in the request.
   */
  def runService(namedParams: Option[java.util.Map[String,AnyRef]] = None): Observable[T] = {
    executeRemoteMethod(CortexConstants.PROLINE_PROCESS_METHOD_NAME, namedParams)
  }
  
}