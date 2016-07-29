package fr.proline.cortex.client.context

import javax.jms._

import com.typesafe.scalalogging.StrictLogging

import org.hornetq.api.core.TransportConfiguration
import org.hornetq.api.jms.HornetQJMSClient
import org.hornetq.api.jms.JMSFactoryType
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory

import fr.profi.util.StringUtils
import fr.proline.cortex.api.JMSConnectionConfig

/**
 * Create a new JMSContext for the Scala client.
 * For detailed information about JMS principles see: http://www.precisejava.com/javaperf/j2ee/JMS.htm
 */
object BuildJMSContext extends StrictLogging {
 
  def apply(config: JMSConnectionConfig): JMSContext = {
    require( StringUtils.isNotEmpty(config.serverHost), "serverHost not defined !")
    
    // Instantiate the TransportConfiguration object
    // which contains the knowledge of which transport to use, the server port etc.
    val transportConfigParams = config.toTransportConfigParams()

    val transportConfiguration = new TransportConfiguration(
      classOf[NettyConnectorFactory].getName(),
      transportConfigParams
    )

    // Instantiate the JMS ConnectionFactory object using this TransportConfiguration
    val connFactory = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration)
      
    val jmsContext = try {
      // Create a JMS v2.0 Context
      connFactory.createContext(Session.AUTO_ACKNOWLEDGE)
    } catch {
      case newConnError: Exception => {
        val errorMsg = "JMS connection creation failed !"
        logger.error(errorMsg)
        throw new Exception(errorMsg, newConnError )
      }
    }
    
    // Automatically start the underlying connection as soon as a message consumer is created
    jmsContext.setAutoStart(true)
    
    jmsContext
  }
  
  /*def close() = synchronized {

    if (_isConnected) {
      try {
        jmsContext.close()

        logger.info("JMS connection successfully closed !")
      } catch {
        case closeConnError: Exception => {
          logger.error("Error closing JMS Connection !")
          throw closeConnError
        }
      }
      finally {
        _isConnected = false
      }
    }

  }*/

}
