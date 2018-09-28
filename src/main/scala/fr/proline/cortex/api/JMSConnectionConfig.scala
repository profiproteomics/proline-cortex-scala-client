package fr.proline.cortex.api

import org.hornetq.core.remoting.impl.netty.TransportConstants

case class JMSConnectionConfig(
  serverHost: String = "localhost",
  serverPort: Int = 5445,
  consumerWindowSize: Int = 0, // disable client-side messages buffering (all messages remains on the server)
  
  // See: https://docs.jboss.org/jbossmessaging/docs/usermanual-2.0.0.beta1/html/client-reconnection.html
  reconnectAttempts: Int = 10 // max. number of allowed reconnections
  
  // FIXME: these parameters don't seem to work properly => why?
  // This optional parameter determines the period in milliseconds between subsequent reconnection attempts, if the connection to the target server has failed.
  // The default value is 2000 milliseconds.
  //retryInterval: Int = 2000, 
  // This optional parameter determines determines a multiplier to apply to the time since the last retry to compute the time to the next retry.
  //retryIntervalMultiplier: Float = 1.0f
) {
  
  def toTransportConfigParams(): java.util.Map[String,Object] = {
    
    val transportConfigParams = new java.util.HashMap[String,Object]()
    
    /* AddJMS Server hostname or IP */
    transportConfigParams.put(TransportConstants.HOST_PROP_NAME, serverHost)
    
    /* Add JMS port */
    transportConfigParams.put(TransportConstants.PORT_PROP_NAME, Integer.valueOf(serverPort))

    transportConfigParams
  }
}