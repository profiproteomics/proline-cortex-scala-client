package fr.proline.cortex.client.profile

/**
 * Profile of the Cortex server targeted by the client.
 *
 * The profile does not change the requests sent by the callers: it is used as a safety guard,
 * to prevent sending requests that the targeted server can't process correctly.
 * Callers of the package fr.proline.cortex.client.compat.v2_1 must be used to call services
 * whose API differs from the one of the latest Cortex server.
 *
 * The profile is mandatory when building a JMSServiceCallerContext (no default value on purpose).
 */
sealed trait CortexProfile {

  /** Name of the profile (used for configuration and logging) */
  def name: String

  /** True if the server still implements the ComputeQuantProfiles service (removed after ProlineSuite 2.1) */
  def supportsComputeQuantProfilesService: Boolean

  /**
   * Version of the Quantify service called by the client with this profile.
   * Version 3.0 expects a LabelFreeQuantConfig 2.0 (a Cortex 2.1 reads it as is, a newer Cortex converts it and fails with
   * the other configurations), version 4.0 expects a LabelFreeQuantConfig 3.0 (proline-core model for Scala 2.13).
   */
  def quantifyServiceVersion: String

  /** Version of the LabelFreeQuantConfig sent to the Quantify service (2.0: package compat.v2_1.config, 3.0: proline-core) */
  def labelFreeQuantConfigVersion: String

  override def toString: String = name
}

object CortexProfile {

  /** Cortex server of ProlineSuite 2.1 */
  case object V2_1 extends CortexProfile {
    val name = "2.1"
    val supportsComputeQuantProfilesService = true
    val quantifyServiceVersion = "3.0"
    val labelFreeQuantConfigVersion = "2.0"
  }

  /** Cortex server built from the latest proline-server sources */
  case object Latest extends CortexProfile {
    val name = "latest"
    val supportsComputeQuantProfilesService = false
    val quantifyServiceVersion = "4.0"
    val labelFreeQuantConfigVersion = "3.0"
  }

  val values: Seq[CortexProfile] = Seq(V2_1, Latest)

  /** Get a profile from its name (case insensitive), e.g. "2.1" or "latest" */
  def fromName(name: String): CortexProfile = {
    require(name != null, "Cortex profile name is null")
    values.find(_.name.equalsIgnoreCase(name.trim)).getOrElse(
      throw new IllegalArgumentException(
        s"unknown Cortex profile '$name' (valid values are: ${values.map(_.name).mkString(", ")})"
      )
    )
  }
}
