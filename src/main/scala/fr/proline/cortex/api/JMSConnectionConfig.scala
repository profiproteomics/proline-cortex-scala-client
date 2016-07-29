package fr.proline.cortex.api

import org.hornetq.core.remoting.impl.netty.TransportConstants
//import fr.proline.cortex.api.CortexConstants._

case class JMSConnectionConfig(
  serverHost: String = "localhost",
  serverPort: Int = 5445
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