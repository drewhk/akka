package akka.remote.artery

import akka.testkit.AkkaSpec

trait AFRAwareTest { self: AkkaSpec ⇒
  val afr = FlightRecorderExtension(system)
  val afrReader = new FlightRecorderReader(afr.fileChannel)

  def dumpEvents(level: Int): Unit = {
    afr.flush()
    // TODO: Find live
    afrReader.rereadStructure()
    afrReader.allEntries(0, level).foreach(println)
  }

}
