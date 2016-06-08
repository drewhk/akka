package akka.remote.artery

object FlightRecorderEvents {

  val NoMetaData = Array.empty[Byte]

  // Top level remoting events
  val Transport_MediaDriverStarted = 0
  val Transport_AeronStarted = 1
  val Transport_AeronErrorLogStarted = 2
  val Transport_TaskRunnerStarted = 3
  val Transport_UniqueAddressSet = 4
  val Transport_MaterializerStarted = 5
  val Transport_StartupFinished = 6
  val Transport_OnAvailableImage = 7
  val Transport_KillSwitchPulled = 8
  val Transport_Stopped = 9
  val Transport_AeronErrorLogTaskStopped = 10
  val Transport_MediaFileDeleted = 11
  val Transport_FlightRecorderClose = 12

  // Aeron Sink events
  val AeronSink_Started = 13
  val AeronSink_TaskRunnerRemoved = 14
  val AeronSink_PublicationClosed = 15
  val AeronSink_Stopped = 16
  val AeronSink_EnvelopeGrabbed = 17
  val AeronSink_EnvelopeOffered = 18
  val AeronSink_GaveUpEnvelope = 19
  val AeronSink_DelegateToTaskRunner = 20

  // Aeron Source events
  val AeronSource_Started = 21
  val AeronSource_Stopped = 22
  val AeronSource_Received = 23
  val AeronSource_DelegateToTaskRunner = 24

  val humandReadable: Map[Int, String] = Map(
    Transport_MediaDriverStarted → "Media driver started",
    Transport_AeronStarted → "Aeron started",
    Transport_AeronErrorLogStarted → "Aeron error log started",
    Transport_TaskRunnerStarted → "Task runner started",
    Transport_UniqueAddressSet → "Unique address set",
    Transport_MaterializerStarted → "Materializer started",
    Transport_StartupFinished → "Transport startup finished",
    Transport_OnAvailableImage → "Image available",
    Transport_KillSwitchPulled → "Transport KillSwitch pulled",
    Transport_Stopped → "Transport stopped",
    Transport_AeronErrorLogTaskStopped → "Aeron error log task stopped",
    Transport_MediaFileDeleted → "Media file deleted",
    Transport_FlightRecorderClose → "Flight recorder closed",

    // Aeron Sink events
    AeronSink_Started → "Aeron sink started",
    AeronSink_TaskRunnerRemoved → "Sink removed from task runner",
    AeronSink_PublicationClosed → "Publication closed",
    AeronSink_Stopped → "Aeron sink stopped",
    AeronSink_EnvelopeGrabbed → "Aeron sink grabbed envelope",
    AeronSink_EnvelopeOffered → "Envelope successfully offered",
    AeronSink_GaveUpEnvelope → "Sink gave up on envelope",
    AeronSink_DelegateToTaskRunner → "Send delegated to taskrunner",

    // Aeron Source events
    AeronSource_Started → "Aeron source started",
    AeronSource_Stopped → "Aeron source stopped",
    AeronSource_Received → "Aeron source received envelope",
    AeronSource_DelegateToTaskRunner → "Aeron source delgated polling to task runner"
  )

}
