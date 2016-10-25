package fr.proline.cortex.api

import org.hornetq.core.remoting.impl.netty.TransportConstants

case class JMSConnectionConfig(
  serverHost: String = "localhost",
  serverPort: Int = 5445,
  consumerWindowSize: Int = 0, // disable client-side messages buffering (all messages remains on the server)
  reconnectAttempts: Int = 10 // max. number of allowed reconnections
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