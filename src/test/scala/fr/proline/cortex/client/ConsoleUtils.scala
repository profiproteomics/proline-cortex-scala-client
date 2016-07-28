package fr.proline.cortex.client

import java.io.IOException
import com.typesafe.scalalogging.LazyLogging

object ConsoleUtils extends LazyLogging {

  def waitUserInput(): Unit = {
    val bbuf = new Array[Byte](8192) // Read buffer

    try {
      /* Flush stdin */
      if (System.in.available() > 0) {
        System.in.read(bbuf)
      }

      System.out.println("\nPress <CR> to stop Message reception")

      /* Blocking read */
      System.in.read(bbuf)
    } catch {
      case ioEx: IOException =>
        logger.error("Error reading stdin", ioEx)
    }

  }

}
