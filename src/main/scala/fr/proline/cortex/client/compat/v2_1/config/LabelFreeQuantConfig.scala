package fr.proline.cortex.client.compat.v2_1.config

import scala.annotation.meta.field

import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

import fr.proline.core.algo.lcms._
import fr.proline.core.algo.lcms.filtering.Filter
import fr.proline.core.algo.msq.config.{ExtractionParams, IMsQuantConfig}

/*
 * Label-Free quantification configuration as expected by a Cortex server of ProlineSuite 2.1 ("config_version": "2.0").
 *
 * These classes are copies of the proline-core 2.1.0 model (package fr.proline.core.algo.msq.config and
 * fr.proline.core.algo.lcms) whose JSON representation has changed in proline-core for Scala 2.13 (config version 3.0).
 * They must be used with a CortexProfile.V2_1 and should be removed once the Cortex server has been upgraded.
 *
 * The following classes of proline-core are still compatible with Cortex 2.1 and are reused as is:
 * ExtractionParams, ClusteringParams, DetectionParams, FeatureMappingParams, AlignmentParams, AlnSmoothingParams
 * and the enumerations of fr.proline.core.algo.lcms.
 */

/** Differences with proline-core 2.13: ftMappingMethodParams (was renamed ftMappingParams) and smoothing method specified here */
case class AlignmentConfig(
  @(JsonScalaEnumeration @field)(classOf[AlnMethodRef])
  methodName: AlnMethod.Value,
  methodParams: Option[AlignmentParams],
  @(JsonScalaEnumeration @field)(classOf[AlnSmoothingRef])
  smoothingMethodName: AlnSmoothing.Value,
  smoothingMethodParams: Option[AlnSmoothingParams] = None,
  @(JsonScalaEnumeration @field)(classOf[FeatureMappingMethodRef])
  ftMappingMethodName: FeatureMappingMethod.Value,
  ftMappingMethodParams: FeatureMappingParams,
  @com.fasterxml.jackson.databind.annotation.JsonDeserialize(contentAs = classOf[java.lang.Boolean])
  removeOutliers: Option[Boolean] = None,
  @com.fasterxml.jackson.databind.annotation.JsonDeserialize(contentAs = classOf[java.lang.Boolean])
  ignoreErrors: Option[Boolean] = None
)

/** Difference with proline-core 2.13: ftMappingParams is a FeatureMappingParams (no automatic time tolerance or m/z calibration) */
case class CrossAssignmentConfig(
  @(JsonScalaEnumeration @field)(classOf[CrossAssignMethodRef])
  methodName: CrossAssignMethod.Value,
  ftMappingParams: FeatureMappingParams,
  restrainToReliableFeatures: Boolean = true,
  ftFilter: Option[Filter] = None
)

/**
 * Differences with proline-core 2.13: no m/z calibration smoothing method
 * and the summarizing method is named "pep_ion_summarizing_methdd" (sic).
 */
case class LabelFreeQuantConfig(
  override val configVersion: String = "2.0",
  extractionParams: ExtractionParams,
  clusteringParams: ClusteringParams,
  alignmentConfig: Option[AlignmentConfig],
  crossAssignmentConfig: Option[CrossAssignmentConfig],
  @(JsonScalaEnumeration @field)(classOf[NormalizationMethodRef])
  normalizationMethod: Option[NormalizationMethod.Value] = None,
  @(JsonScalaEnumeration @field)(classOf[DetectionMethodRef])
  detectionMethodName: DetectionMethod.Value,
  detectionParams: Option[DetectionParams] = None,
  useLastPeakelDetection: Boolean = false,
  @(JsonScalaEnumeration @field)(classOf[MqPepIonAbundanceSummarizingMethodRef])
  pepIonSummarizingMethdd: Option[MqPepIonAbundanceSummarizingMethod.Value] = None
) extends IMsQuantConfig
