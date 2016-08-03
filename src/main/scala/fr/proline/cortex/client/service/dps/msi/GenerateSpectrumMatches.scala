package fr.proline.cortex.client.service.dps.msi

import scala.collection.mutable.HashMap

import fr.profi.util.primitives.toBoolean
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client.AbstractRemoteProcessingServiceCaller
import fr.proline.cortex.client.IRemoteServiceConfig
import fr.proline.cortex.client.context.RemoteServiceCallerContext

import rx.lang.scala.Observable

case class GenerateSpectrumMatchesConfig(
  resultSetId: Long,
  resultSummaryId: Option[Long],
  peptideMatchIds: Option[Seq[Long]],
  forceInsert: Option[Boolean] = None
) extends IRemoteServiceConfig {
  
  import GenerateSpectrumMatchesService.PROCESS_METHOD._
  
  def putInNamedParamMap(namedParams: HashMap[String,Any] ): HashMap[String,Any] = {
    namedParams.put(RESULT_SET_ID_PARAM, resultSetId)
    if (resultSummaryId.isDefined) namedParams.put(RESULT_SUMMARY_ID_PARAM, resultSummaryId.get)
    if (peptideMatchIds.isDefined) namedParams.put(PEPTIDE_MATCH_IDS_PARAM, peptideMatchIds.get)
    if (forceInsert.isDefined) namedParams.put(FORCE_INSERT_PARAM, forceInsert.get)
    
    namedParams
  }
}

object GenerateSpectrumMatches extends AbstractRemoteProcessingServiceCaller[Boolean] with IGenerateSpectrumMatchesService {
  
  import this.PROCESS_METHOD._
  
  def apply(projectId: Long, config: GenerateSpectrumMatchesConfig)(implicit callerCtx: RemoteServiceCallerContext): Observable[Boolean] = {
    
    val namedParams = config.putInNamedParamMap( new HashMap[String,Any]() )
    namedParams.put(PROJECT_ID_PARAM, projectId)
    
    this.runService( Some(namedParams) )
  }
  
  protected def convertResponseResult(responseResult: AnyRef): Boolean = toBoolean(responseResult)
  
}