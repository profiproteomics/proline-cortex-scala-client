# Proline-Cortex-Scala-Client

Scala client library for Proline Cortex, the JMS server of the Proline suite.
It lets an application call the Proline Server services remotely, over JMS, without depending on the server code.

The services currently covered are grouped as in Proline-Cortex-API:
  * Proline administration: users, projects and databases
  * Identification data processing (MSI): import of result files, validation, merge, export, spectrum matches generation, MS diagnostics, MaxQuant import...
  * Quantification data processing (MSQ): quantification, post-processing and aggregation
  * User data space (UDS): raw file registration, identification tree validation, export information
  * Miscellaneous: remote file system access and service management

The sources of the server side are hosted in [Proline-Server](https://github.com/profiproteomics/proline-server) (Proline-Cortex-API and Proline-Cortex).

# Build

The project is built with Maven. It depends on the Proline-Cortex-API and Proline-Databases modules,
and on the Profi-Commons-Scala library.

# License

This project is licensed under the [CeCILL License V2.1](http://www.cecill.info/licences/Licence_CeCILL_V2.1-en.html)

# Proline Web Site

 Visit http://www.profiproteomics.fr/proline for Proline Suite documentation and downloads.