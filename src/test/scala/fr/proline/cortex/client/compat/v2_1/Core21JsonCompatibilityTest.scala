package fr.proline.cortex.client.compat.v2_1

import org.junit.Assert._
import org.junit.Test

import com.fasterxml.jackson.databind.JsonNode

import fr.profi.util.serialization.ProfiJson
import fr.proline.core.algo.lcms.{AlignmentConfig => _, CrossAssignmentConfig => _, _}
import fr.proline.core.algo.lcms.filtering.Filter
import fr.proline.core.algo.msq.config.profilizer.{ProfilizerConfig, ProfilizerStatConfig}
import fr.proline.core.algo.msq.config.{ExtractionParams, MzToleranceParams}
import fr.proline.cortex.client.compat.v2_1.config._

/**
 * Checks that the JSON of the configurations sent to a Cortex 2.1 is identical to the one produced by
 * the classes of proline-core 2.1.0 (reference files in src/test/resources/compat/v2_1).
 */
class Core21JsonCompatibilityTest {

  private def readResource(name: String): JsonNode = {
    val is = getClass.getResourceAsStream("/compat/v2_1/" + name)
    require(is != null, s"missing test resource $name")
    try ProfiJson.getObjectMapper().readTree(is) finally is.close()
  }

  private def toTree(value: Any): JsonNode = ProfiJson.getObjectMapper().readTree(ProfiJson.serialize(value))

  private def assertSameJson(resourceName: String, value: Any): Unit = {
    assertEquals(s"JSON differs from the proline-core 2.1.0 reference $resourceName", readResource(resourceName), toTree(value))
  }

  @Test
  def labelFreeConfigFull(): Unit = {
    val config = LabelFreeQuantConfig(
      extractionParams = ExtractionParams(5.0, "ppm"),
      clusteringParams = ClusteringParams(5.0, "ppm", 15f, "MOST_INTENSE", "MOST_INTENSE"),
      alignmentConfig = Some(AlignmentConfig(
        methodName = AlnMethod.ITERATIVE,
        methodParams = Some(AlignmentParams(Some(20000), Some(3))),
        smoothingMethodName = AlnSmoothing.LANDMARK_RANGE,
        smoothingMethodParams = Some(AlnSmoothingParams(200, 50, Some(50))),
        ftMappingMethodName = FeatureMappingMethod.FEATURE_COORDINATES,
        ftMappingMethodParams = FeatureMappingParams(Some(5.0), Some("ppm"), 600f),
        removeOutliers = Some(true),
        ignoreErrors = Some(false)
      )),
      crossAssignmentConfig = Some(CrossAssignmentConfig(
        methodName = CrossAssignMethod.BETWEEN_ALL_RULS,
        ftMappingParams = FeatureMappingParams(Some(5.0), Some("ppm"), 60f),
        restrainToReliableFeatures = true,
        ftFilter = Some(Filter("intensity", "GTE", 1000.0))
      )),
      normalizationMethod = Some(NormalizationMethod.MEDIAN_RATIO),
      detectionMethodName = DetectionMethod.DETECT_FEATURES,
      detectionParams = Some(DetectionParams(
        startFromValidatedPeptides = Some(true),
        psmMatchingParams = Some(MzToleranceParams(5.0, "ppm")),
        ftMappingParams = Some(FeatureMappingParams(Some(5.0), Some("ppm"), 120f)),
        isotopeMatchingParams = Some(MzToleranceParams(5.0, "ppm"))
      )),
      useLastPeakelDetection = true,
      pepIonSummarizingMethdd = Some(MqPepIonAbundanceSummarizingMethod.BEST_ION)
    )
    assertSameJson("labelfree_config_full.json", config)
  }

  @Test
  def labelFreeConfigMinimal(): Unit = {
    val config = LabelFreeQuantConfig(
      extractionParams = ExtractionParams(10.0, "ppm"),
      clusteringParams = ClusteringParams(10.0, "ppm", 15f, "MOST_INTENSE", "MOST_INTENSE"),
      alignmentConfig = None,
      crossAssignmentConfig = None,
      detectionMethodName = DetectionMethod.EXTRACT_IONS
    )
    assertSameJson("labelfree_config_min.json", config)
  }

  @Test
  def profilizerConfigDefault(): Unit = {
    assertSameJson("profilizer_config_default.json", ProfilizerConfig())
  }

  @Test
  def profilizerConfigCustom(): Unit = {
    val config = ProfilizerConfig(
      discardMissedCleavedPeptides = false,
      discardOxidizedPeptides = false,
      useOnlySpecificPeptides = false,
      discardPeptidesSharingPeakels = false,
      applyProfileClustering = false,
      abundanceSummarizerMethod = "SUM",
      peptideStatConfig = ProfilizerStatConfig(statTestsAlpha = 0.05f, applyNormalization = false),
      proteinStatConfig = ProfilizerStatConfig(minZScore = 0.5f)
    )
    assertSameJson("profilizer_config_custom.json", config)
  }

  /** Documents why the LabelFreeQuantConfig of proline-core 2.13 can't be sent to a Cortex 2.1 (see CortexProfile) */
  @Test
  def coreLabelFreeConfigV3IsNotReadableByCortex21(): Unit = {
    import fr.proline.core.algo.msq.config.LabelFreeQuantConfig
    val v3 = toTree(LabelFreeQuantConfig(
      extractionParams = ExtractionParams(10.0, "ppm"),
      clusteringParams = ClusteringParams(10.0, "ppm", 15f, "MOST_INTENSE", "MOST_INTENSE"),
      alignmentConfig = None,
      mozCalibrationSmoothingMethod = MozCalibrationSmoothing.LOESS,
      crossAssignmentConfig = None,
      detectionMethodName = DetectionMethod.EXTRACT_IONS
    ))
    assertEquals("3.0", v3.get("config_version").asText())
    assertTrue(v3.has("moz_calibration_smoothing_method"))
    assertNotEquals(readResource("labelfree_config_min.json"), v3)
  }
}
