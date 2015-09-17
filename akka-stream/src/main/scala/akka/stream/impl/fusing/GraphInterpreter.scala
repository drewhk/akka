/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl.fusing

import akka.stream.stage.{ OutHandler, InHandler, GraphStage, GraphStageLogic }
import akka.stream.{ Shape, Inlet, Outlet }

/**
 * INTERNAL API
 *
 * (See the class for the documentation of the internals)
 */
private[stream] object GraphInterpreter {
  /**
   * Compile time constant, enable it for debug logging to the console.
   */
  final val Debug = false

  /**
   * Marker object that indicates that a port holds no element since it was already grabbed. The port is still pullable,
   * but there is no more element to grab.
   */
  case object Empty

  sealed trait ConnectionState
  sealed trait CompletedState extends ConnectionState
  case object Pushable extends ConnectionState
  case object Completed extends CompletedState
  final case class PushCompleted(element: Any) extends ConnectionState
  case object Cancelled extends CompletedState
  final case class Failed(ex: Throwable) extends CompletedState

  val NoEvent = -1
  val Boundary = -1

  abstract class UpstreamBoundaryStageLogic[T] extends GraphStageLogic {
    def out: Outlet[T]
  }

  abstract class DownstreamBoundaryStageLogic[T] extends GraphStageLogic {
    def in: Inlet[T]
  }

  /**
   * INTERNAL API
   *
   * A GraphAssembly represents a small stream processing graph to be executed by the interpreter. Instances of this
   * class **must not** be mutated after construction.
   *
   * The arrays [[ins]] and [[outs]] correspond to the notion of a *connection* in the [[GraphInterpreter]]. Each slot
   * *i* contains the input and output port corresponding to connection *i*. Slots where the graph is not closed (i.e.
   * ports are exposed to the external world) are marked with *null* values. For example if an input port *p* is
   * exposed, then outs(p) will contain a *null*.
   *
   * The arrays [[inOwners]] and [[outOwners]] are lookup tables from a connection id (the index of the slot)
   * to a slot in the [[stages]] array, indicating which stage is the owner of the given input or output port.
   * Slots which would correspond to non-existent stages (where the corresponding port is null since it represents
   * the currently unknown external context) contain the value [[GraphInterpreter#Boundary]].
   *
   * The current assumption by the infrastructure is that the layout of these arrays looks like this:
   *
   *            +---------------------------------------+-----------------+
   * inOwners:  | index to stages array                 | Boundary (-1)   |
   *            +----------------+----------------------+-----------------+
   * ins:       | exposed inputs | internal connections | nulls           |
   *            +----------------+----------------------+-----------------+
   * outs:      | nulls          | internal connections | exposed outputs |
   *            +----------------+----------------------+-----------------+
   * outOwners: | Boundary (-1)  | index to stages array                  |
   *            +----------------+----------------------------------------+
   *
   * In addition, it is also assumed by the infrastructure that the order of exposed inputs and outputs in the
   * corresponding segments of these arrays matches the exact same order of the ports in the [[Shape]].
   *
   */
  final case class GraphAssembly(stages: Array[GraphStage[_]],
                                 ins: Array[Inlet[_]],
                                 inOwners: Array[Int],
                                 outs: Array[Outlet[_]],
                                 outOwners: Array[Int]) {

    val connectionCount: Int = ins.length

    /**
     * Takes an interpreter and returns three arrays required by the interpreter containing the input, output port
     * handlers and the stage logic instances.
     */
    def materialize(interpreter: GraphInterpreter): (Array[InHandler], Array[OutHandler], Array[GraphStageLogic]) = {
      val logics = Array.ofDim[GraphStageLogic](stages.length)
      for (i ← stages.indices) {
        logics(i) = stages(i).createLogic
        logics(i).interpreter = interpreter
      }

      val inHandlers = Array.ofDim[InHandler](connectionCount)
      val outHandlers = Array.ofDim[OutHandler](connectionCount)

      for (i ← 0 until connectionCount) {
        if (ins(i) ne null) {
          inHandlers(i) = logics(inOwners(i)).inHandlers(ins(i))
          logics(inOwners(i)).inToConn += ins(i) -> i
        }
        if (outs(i) ne null) {
          outHandlers(i) = logics(outOwners(i)).outHandlers(outs(i))
          logics(outOwners(i)).outToConn += outs(i) -> i
        }
      }

      (inHandlers, outHandlers, logics)
    }

    override def toString: String =
      "GraphAssembly(" +
        stages.mkString("[", ",", "]") + ", " +
        ins.mkString("[", ",", "]") + ", " +
        inOwners.mkString("[", ",", "]") + ", " +
        outs.mkString("[", ",", "]") + ", " +
        outOwners.mkString("[", ",", "]") +
        ")"
  }
}

/**
 * INERNAL API
 *
 * From an external viewpoint, the GraphInterpreter takes an assembly of graph processing stages encoded as a
 * [[GraphInterpreter#GraphAssembly]] object and provides facilities to execute and interact with this assembly.
 * The lifecylce of the Interpreter is roughly the following:
 *  - Boundary logics are attached via [[attachDownstreamBoundary()]] and [[attachUpstreamBoundary()]]
 *  - [[init()]] is called
 *  - [[execute()]] is called whenever there is need for execution, providing an upper limit on the processed events
 *  - [[finish()]] is called before the interpreter is disposed, preferably after [[isCompleted]] returned true, although
 *    in abort cases this is not strictly necessary
 *
 * The [[execute()]] method of the interpreter accepts an upper bound on the events it will process. After this limit
 * is reached or there are no more pending events to be processed, the call returns. It is possible to inspect
 * if there are unprocessed events left via the [[isSuspended]] method. [[isCompleted]] returns true once all stages
 * reported completion inside the interpreter.
 *
 * The internal architecture of the interpreter is based on the usage of arrays and optimized for reducing allocations
 * on the hot paths.
 *
 * One of the basic abstractions inside the interpreter is the notion of *connection*. In the abstract sense a
 * connection represents an output-input port pair (an analogue for a connected RS Publisher-Subscriber pair),
 * while in the practical sense a connection is a number which represents slots in certain arrays.
 * In particular
 *  - connectionStates is a mapping from a connection id to a current (or future) state of the connection
 *  - inAvailable is a mapping from a connection to a boolean that indicates whether the input corresponding
 *    to the connection is currently pullable
 *  - outAvailable is a mapping from a connection to a boolean that indicates whether the input corresponding
 *    to the connection is currently pushable
 *  - inHandlers is a mapping from a connection id to the [[InHandler]] instance that handles the events corresponding
 *    to the input port of the connection
 *  - outHandlers is a mapping from a connection id to the [[OutHandler]] instance that handles the events corresponding
 *    to the output port of the connection
 *
 * On top of these lookup tables there is an eventQueue, represented as a circular buffer of integers. The integers
 * it contains represents connections that have pending events to be processed. The pending event itself is encoded
 * in the connectionStates table. This implies that there can be only one event in flight for a given connection, which
 * is true in almost all cases, except a complete-after-push which is therefore handled with a special event
 * [[GraphInterpreter#PushCompleted]].
 *
 * Sending an event is usually the following sequence:
 *  - An action is requested by a stage logic (push, pull, complete, etc.)
 *  - the availability of the port is set on the sender side to false (inAvailable or outAvailable)
 *  - the scheduled event is put in the slot of the connection in the connectionStates table
 *  - the id of the affected connection is enqueued
 *
 * Receiving an event is usually the following sequence:
 *  - id of connection to be processed is dequeued
 *  - the type of the event is determined by the object in the corresponding connectionStates slot
 *  - the availability of the port is set on the receiver side to be true (inAvailable or outAvailable)
 *  - using the inHandlers/outHandlers table the corresponding callback is called on the stage logic.
 *
 * Because of the FIFO construction of the queue the interpreter is fair, i.e. a pending event is always executed
 * after a bounded number of other events. This property, together with suspendability means that even infinite cycles can
 * be modeled, or even dissolved (if preempted and a "stealing" external even is injected; for example the non-cycle
 * edge of a balance is pulled, dissolving the original cycle).
 */
private[stream] final class GraphInterpreter(
  private val assembly: GraphInterpreter.GraphAssembly,
  val onAsyncInput: (GraphStageLogic, Any, (Any) ⇒ Unit) ⇒ Unit) {
  import GraphInterpreter._

  // Maintains the next event (and state) of the connection.
  // Technically the connection cannot be considered being in the state that is encoded here before the enqueued
  // connection event has been processed. The inAvailable and outAvailable arrays usually protect access to this
  // field while it is in transient state.
  val connectionStates = Array.fill[Any](assembly.connectionCount)(Empty)

  // Indicates whether the input port is pullable. After pulling it becomes false
  // Be aware that when inAvailable goes to false outAvailable does not become true immediately, only after
  // the corresponding event in the queue has been processed
  val inAvailable = Array.fill[Boolean](assembly.connectionCount)(true)

  // Indicates whether the output port is pushable. After pushing it becomes false
  // Be aware that when inAvailable goes to false outAvailable does not become true immediately, only after
  // the corresponding event in the queue has been processed
  val outAvailable = Array.fill[Boolean](assembly.connectionCount)(false)

  // Lookup tables for the InHandler and OutHandler for a given connection ID, and a lookup table for the
  // GraphStageLogic instances
  val (inHandlers, outHandlers, logics) = assembly.materialize(this)

  // The number of currently running stages. Once this counter reaches zero, the interpreter is considered to be
  // completed
  private var runningStages = assembly.stages.length

  // Counts how many active connections a stage has. Once it reaches zero, the stage is automatically stopped.
  private val shutdownCounter = Array.tabulate(assembly.stages.length) { i ⇒
    val shape = assembly.stages(i).shape.asInstanceOf[Shape]
    shape.inlets.size + shape.outlets.size
  }

  // An event queue implemented as a circular buffer
  private val mask = 255
  private val eventQueue = Array.ofDim[Int](256)
  private var queueHead: Int = 0
  private var queueTail: Int = 0

  /**
   * Assign the boundary logic to a given connection. This will serve as the interface to the external world
   * (outside the interpreter) to process and inject events.
   */
  def attachUpstreamBoundary(connection: Int, logic: UpstreamBoundaryStageLogic[_]): Unit = {
    logic.outToConn += logic.out -> connection
    logic.interpreter = this
    outHandlers(connection) = logic.outHandlers.head._2
  }

  /**
   * Assign the boundary logic to a given connection. This will serve as the interface to the external world
   * (outside the interpreter) to process and inject events.
   */
  def attachDownstreamBoundary(connection: Int, logic: DownstreamBoundaryStageLogic[_]): Unit = {
    logic.inToConn += logic.in -> connection
    logic.interpreter = this
    inHandlers(connection) = logic.inHandlers.head._2
  }

  /**
   * Returns true if there are pending unprocessed events in the event queue.
   */
  def isSuspended: Boolean = queueHead != queueTail

  /**
   * Returns true if there are no more running stages and pending events.
   */
  def isCompleted: Boolean = runningStages == 0 && !isSuspended

  /**
   * Initializes the states of all the stage logics by calling preStart()
   */
  def init(): Unit = {
    var i = 0
    while (i < logics.length) {
      logics(i).stageId = i
      logics(i).preStart()
      i += 1
    }
  }

  /**
   * Finalizes the state of all stages by calling postStop() (if necessary).
   */
  def finish(): Unit = {
    var i = 0
    while (i < logics.length) {
      if (!isStageCompleted(i)) logics(i).postStop()
      i += 1
    }
  }

  // Debug name for a connections input part
  private def inOwnerName(connection: Int): String =
    if (assembly.inOwners(connection) == Boundary) "DownstreamBoundary"
    else assembly.stages(assembly.inOwners(connection)).toString

  // Debug name for a connections ouput part
  private def outOwnerName(connection: Int): String =
    if (assembly.outOwners(connection) == Boundary) "UpstreamBoundary"
    else assembly.stages(assembly.outOwners(connection)).toString

  /**
   * Executes pending events until the given limit is met. If there were remaining events, isSuspended will return
   * true.
   */
  def execute(eventLimit: Int): Unit = {
    var eventsRemaining = eventLimit
    var connection = dequeue()
    while (eventsRemaining > 0 && connection != NoEvent) {
      processEvent(connection)
      eventsRemaining -= 1
      if (eventsRemaining > 0) connection = dequeue()
    }
    // TODO: deadlock detection
  }

  // Decodes and processes a single event for the given connection
  private def processEvent(connection: Int): Unit = {

    def processElement(elem: Any): Unit = {
      if (!isStageCompleted(assembly.inOwners(connection))) {
        if (GraphInterpreter.Debug) println(s"PUSH ${outOwnerName(connection)} -> ${inOwnerName(connection)}, $elem")
        inAvailable(connection) = true
        inHandlers(connection).onPush()
      }
    }

    connectionStates(connection) match {
      case Pushable ⇒
        if (!isStageCompleted(assembly.outOwners(connection))) {
          if (GraphInterpreter.Debug) println(s"PULL ${inOwnerName(connection)} -> ${outOwnerName(connection)}")
          outAvailable(connection) = true
          outHandlers(connection).onPull()
        }
      case Completed ⇒
        val stageId = assembly.inOwners(connection)
        if (!isStageCompleted(stageId)) {
          if (GraphInterpreter.Debug) println(s"COMPLETE ${outOwnerName(connection)} -> ${inOwnerName(connection)}")
          inAvailable(connection) = false
          inHandlers(connection).onUpstreamFinish()
          completeConnection(stageId)
        }
      case Failed(ex) ⇒
        val stageId = assembly.inOwners(connection)
        if (!isStageCompleted(stageId)) {
          if (GraphInterpreter.Debug) println(s"FAIL ${outOwnerName(connection)} -> ${inOwnerName(connection)}")
          inAvailable(connection) = false
          inHandlers(connection).onUpstreamFailure(ex)
          completeConnection(stageId)
        }
      case Cancelled ⇒
        val stageId = assembly.outOwners(connection)
        if (!isStageCompleted(stageId)) {
          if (GraphInterpreter.Debug) println(s"CANCEL ${inOwnerName(connection)} -> ${outOwnerName(connection)}")
          outAvailable(connection) = false
          outHandlers(connection).onDownstreamFinish()
          completeConnection(stageId)
        }
      case PushCompleted(elem) ⇒
        inAvailable(connection) = true
        connectionStates(connection) = elem
        processElement(elem)
        enqueue(connection, Completed)
      case pushedElem ⇒ processElement(pushedElem)

    }

  }

  private def dequeue(): Int = {
    if (queueHead == queueTail) NoEvent
    else {
      val idx = queueHead & mask
      val elem = eventQueue(idx)
      eventQueue(idx) = NoEvent
      queueHead += 1
      elem
    }
  }

  private def enqueue(connection: Int, event: Any): Unit = {
    connectionStates(connection) = event
    eventQueue(queueTail & mask) = connection
    queueTail += 1
  }

  // Returns true if a connection has been completed *or if the completion event is already enqueued*. This is useful
  // to prevent redundant completion events in case of concurrent invocation on both sides of the connection.
  // I.e. when one side already enqueued the completion event, then the other side will not enqueue the event since
  // there is noone to process it anymore.
  def isConnectionCompleted(connection: Int): Boolean = connectionStates(connection).isInstanceOf[CompletedState]

  // Returns true if the given stage is alredy completed
  def isStageCompleted(stageId: Int): Boolean = stageId != Boundary && shutdownCounter(stageId) == 0

  private def isPushInFlight(connection: Int): Boolean =
    !inAvailable(connection) &&
      !connectionStates(connection).isInstanceOf[ConnectionState] &&
      connectionStates(connection) != Empty

  // Register that a connection in which the given stage participated has been completed and therefore the stage
  // itself might stop, too.
  private def completeConnection(stageId: Int): Unit = {
    if (stageId != Boundary) {
      val activeConnections = shutdownCounter(stageId)
      if (activeConnections > 0) {
        shutdownCounter(stageId) = activeConnections - 1
        // This was the last active connection keeping this stage alive
        if (activeConnections == 1) {
          runningStages -= 1
          logics(stageId).postStop()
        }
      }
    }
  }

  private[stream] def push(connection: Int, elem: Any): Unit = {
    outAvailable(connection) = false
    enqueue(connection, elem)
  }

  private[stream] def pull(connection: Int): Unit = {
    inAvailable(connection) = false
    enqueue(connection, Pushable)
  }

  private[stream] def complete(connection: Int): Unit = {
    outAvailable(connection) = false
    if (!isConnectionCompleted(connection)) {
      // There is a pending push, we change the signal to be a PushCompleted (there can be only one signal in flight
      // for a connection)
      if (isPushInFlight(connection))
        connectionStates(connection) = PushCompleted(connectionStates(connection))
      else
        enqueue(connection, Completed)
    }
    completeConnection(assembly.outOwners(connection))
  }

  private[stream] def fail(connection: Int, ex: Throwable): Unit = {
    outAvailable(connection) = false
    if (!isConnectionCompleted(connection)) enqueue(connection, Failed(ex))
    completeConnection(assembly.outOwners(connection))
  }

  private[stream] def cancel(connection: Int): Unit = {
    inAvailable(connection) = false
    if (!isConnectionCompleted(connection)) enqueue(connection, Cancelled)
    completeConnection(assembly.inOwners(connection))
  }

}