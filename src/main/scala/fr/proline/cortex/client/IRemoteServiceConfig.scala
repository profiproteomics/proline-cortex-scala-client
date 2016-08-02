package fr.proline.cortex.client

trait IRemoteServiceConfig {
  def putInNamedParamMap(namedParams: java.util.HashMap[String,AnyRef] ): java.util.HashMap[String,AnyRef]
}