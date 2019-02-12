package fr.proline.cortex.client

import scala.collection.mutable.HashMap

trait INamedParamsBuilder {
  def putInNamedParamMap(namedParams: HashMap[String,Any]): HashMap[String,Any]
}