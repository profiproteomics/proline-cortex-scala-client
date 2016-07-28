package fr.proline.cortex.api

import java.util.Date
import fr.profi.util.lang.EnhancedEnum

object JMSNotificationMessageKey extends EnhancedEnum {
  val SERVICE_NAME = Value("service_name")
  val EVENT_TIMESTAMP = Value("event_timestamp")
  // TODO: find a better name (JMS_REQUEST_MESSAGE_ID)
  val REQUEST_JMS_MESSAGE_ID = Value("request_jms_message_id")
  val JSON_RPC_REQUEST_ID = Value("json_rpc_request_id")
  val EVENT_TYPE = Value("event_type")
}

case class JMSNotificationMessage(
  serviceName: String, 
  eventTimestamp: Long,
  requestJmsMessageId: String,
  jsonRpcRequestId: String,
  eventType: String
) {
  lazy val eventDate = new Date(eventTimestamp)
}
