package fr.proline.cortex.client

import scala.collection.mutable.HashMap

trait IRemoteServiceConfig {
  def putInNamedParamMap(namedParams: HashMap[String,Any] ): HashMap[String,Any]
}