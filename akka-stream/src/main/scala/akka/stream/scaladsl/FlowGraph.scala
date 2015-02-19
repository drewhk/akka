/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import akka.stream.impl.Junctions._
import akka.stream.impl.GenJunctions._
import akka.stream.impl.Stages.{ MaterializingStageFactory, StageModule }
import akka.stream.impl._
import akka.stream.impl.StreamLayout._
import akka.stream.scaladsl.FlowGraph.FlowGraphBuilder
import akka.stream.scaladsl.Graphs.{ InPort, OutPort }

import scala.collection.immutable

/**
 * Merge several streams, taking elements as they arrive from input streams
 * (picking randomly when several have elements ready).
 *
 * A `Merge` has one `out` port and one or more `in` ports.
 */
object Merge {

  final case class MergePorts[T](in: Vector[InPort[T]], out: OutPort[T]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = in
    override val outlets: immutable.Seq[OutPort[_]] = List(out)

    override def deepCopy(): MergePorts[T] = MergePorts(in.map(i ⇒ new InPort[T](i.toString)), new OutPort(out.toString))
  }

  /**
   * Create a new `Merge` with the specified number of input ports and attributes.
   *
   * @param inputPorts number of input ports
   * @param attributes optional attributes
   */
  def apply[T](inputPorts: Int, attributes: OperationAttributes = OperationAttributes.none)(implicit b: FlowGraphBuilder): MergePorts[T] = {
    require(inputPorts >= 1, s"A Merge must have 1 or more input ports [$inputPorts]")
    val mergeModule = new MergeModule(
      Vector.fill(inputPorts)(new InPort[T]("Merge.in")),
      new OutPort[T]("Merge.out"),
      OperationAttributes.name("Merge") and attributes)
    b.addModule(mergeModule)
    MergePorts(mergeModule.ins, mergeModule.out)
  }

}

/**
 * Merge several streams, taking elements as they arrive from input streams
 * (picking from preferred when several have elements ready).
 *
 * A `MergePreferred` has one `out` port, one `preferred` input port and 0 or more secondary `in` ports.
 */
object MergePreferred {
  final case class MergePreferredPorts[T](preferred: InPort[T], in: Vector[InPort[T]], out: OutPort[T]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = in :+ preferred
    override val outlets: immutable.Seq[OutPort[_]] = List(out)

    override def deepCopy(): MergePreferredPorts[T] =
      MergePreferredPorts(new InPort(preferred.toString), in.map(i ⇒ new InPort[T](i.toString)), new OutPort(out.toString))
  }

  /**
   * Create a new `PreferredMerge` with the specified number of secondary input ports and attributes.
   *
   * @param secondaryPorts number of secondary input ports
   * @param attributes optional attributes
   */
  def apply[T](secondaryPorts: Int, attributes: OperationAttributes = OperationAttributes.none)(implicit b: FlowGraphBuilder): MergePreferredPorts[T] = {
    require(secondaryPorts >= 0, s"A MergePreferred must have 0 or more secondary input ports [$secondaryPorts]")
    val mergeModule = new MergePreferredModule(
      new InPort[T]("Preferred.preferred"),
      Vector.fill(secondaryPorts)(new InPort[T]("Preferred.in")),
      new OutPort[T]("Preferred.out"),
      OperationAttributes.name("MergePreferred") and attributes)
    b.addModule(mergeModule)
    MergePreferredPorts(mergeModule.preferred, mergeModule.ins, mergeModule.out)
  }
}

/**
 * Fan-out the stream to several streams. Each element is produced to
 * the other streams. It will not shut down until the subscriptions
 * for at least two downstream subscribers have been established.
 *
 * A `Broadcast` has one `in` port and 2 or more `out` ports.
 */
object Broadcast {

  final case class BroadcastPorts[T](in: InPort[T], out: Vector[OutPort[T]]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = List(in)
    override val outlets: immutable.Seq[OutPort[_]] = out

    override def deepCopy(): BroadcastPorts[T] =
      BroadcastPorts(new InPort(in.toString), out.map(o ⇒ new OutPort[T](o.toString)))
  }

  /**
   * Create a new `Broadcast` with the specified number of output ports and attributes.
   *
   * @param outputPorts number of output ports
   * @param attributes optional attributes
   */
  def apply[T](outputPorts: Int, attributes: OperationAttributes = OperationAttributes.none)(implicit b: FlowGraphBuilder): BroadcastPorts[T] = {
    require(outputPorts >= 2, s"A Broadcast must have 2 or more output ports [$outputPorts]")
    val bcastModule = new BroadcastModule(
      new InPort[T]("Bcast.in"),
      Vector.fill(outputPorts)(new OutPort[T]("Bcast.out")),
      OperationAttributes.name("Broadcast") and attributes)
    b.addModule(bcastModule)
    BroadcastPorts(bcastModule.in, bcastModule.outs)
  }
}

/**
 * Fan-out the stream to several streams. Each element is produced to
 * one of the other streams. It will not shut down until the subscriptions
 * for at least two downstream subscribers have been established.
 *
 * A `Balance` has one `in` port and 2 or more `out` ports.
 */
object Balance {

  final case class BalancePorts[T](in: InPort[T], out: Vector[OutPort[T]]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = List(in)
    override val outlets: immutable.Seq[OutPort[_]] = out

    override def deepCopy(): BalancePorts[T] =
      BalancePorts(new InPort(in.toString), out.map(o ⇒ new OutPort[T](o.toString)))
  }

  /**
   * Create a new `Balance` with the specified number of output ports and attributes.
   *
   * @param outputPorts number of output ports
   * @param waitForAllDownstreams if you use `waitForAllDownstreams = true` it will not start emitting
   *   elements to downstream outputs until all of them have requested at least one element,
   *   default value is `false`
   * @param attributes optional attributes
   */
  def apply[T](
    outputPorts: Int,
    waitForAllDownstreams: Boolean = false,
    attributes: OperationAttributes = OperationAttributes.none)(implicit b: FlowGraphBuilder): BalancePorts[T] = {
    require(outputPorts >= 2, s"A Balance must have 2 or more output ports [$outputPorts]")
    val bcastModule = new BalanceModule(
      new InPort[T]("Balance.in"),
      Vector.fill(outputPorts)(new OutPort[T]("Balance.out")),
      waitForAllDownstreams,
      OperationAttributes.name("Balance") and attributes)
    b.addModule(bcastModule)
    BalancePorts(bcastModule.in, bcastModule.outs)
  }
}

/**
 * Combine the elements of 2 streams into a stream of tuples.
 *
 * A `Zip` has a `left` and a `right` input port and one `out` port
 */
object Zip {

  final case class ZipPorts[A, B](left: InPort[A], right: InPort[B], out: OutPort[(A, B)]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = List(left, right)
    override val outlets: immutable.Seq[OutPort[_]] = List(out)

    override def deepCopy(): ZipPorts[A, B] =
      ZipPorts(new InPort(left.toString), new InPort(right.toString), new OutPort(out.toString))
  }

  /**
   * Create a new `Zip` with the specified attributes.
   *
   * @param attributes optional attributes
   */
  def apply[A, B](attributes: OperationAttributes = OperationAttributes.none)(implicit b: FlowGraphBuilder): ZipPorts[A, B] = {
    val zipWithModule = new ZipWith2Module(
      new InPort[A]("Zip.left"),
      new InPort[B]("Zip.right"),
      new OutPort[(A, B)]("Zip.out"),
      (a: A, b: B) ⇒ (a, b),
      OperationAttributes.name("Zip") and attributes)
    b.addModule(zipWithModule)
    ZipPorts(zipWithModule.in1, zipWithModule.in2, zipWithModule.out)
  }

}

/**
 * Combine the elements of multiple streams into a stream of the combined elements.
 */
object ZipWith extends ZipWithApply

/**
 * Takes a stream of pair elements and splits each pair to two output streams.
 *
 * An `Unzip` has one `in` port and one `left` and one `right` output port.
 */
object Unzip {

  final case class UnzipPorts[A, B](in: InPort[(A, B)], left: OutPort[A], right: OutPort[B]) extends Graphs.Ports {
    override def inlets: immutable.Seq[InPort[_]] = List(in)
    override def outlets: immutable.Seq[OutPort[_]] = List(left, right)

    override def deepCopy(): UnzipPorts[A, B] =
      UnzipPorts(new InPort(in.toString), new OutPort(left.toString), new OutPort(right.toString))
  }

  /**
   * Create a new `Unzip` with the specified attributes.
   *
   * @param attributes optional attributes
   */
  def apply[A, B](attributes: OperationAttributes = OperationAttributes.none)(implicit b: FlowGraphBuilder): UnzipPorts[A, B] = {
    val unzipModule = new UnzipModule(
      new InPort[(A, B)]("Unzip.in"),
      new OutPort[A]("Unzip.left"),
      new OutPort[B]("Unzip.right"),
      OperationAttributes.name("Unzip") and attributes)
    b.addModule(unzipModule)
    UnzipPorts(unzipModule.in, unzipModule.left, unzipModule.right)
  }
}

/**
 * Takes two streams and outputs one stream formed from the two input streams
 * by first emitting all of the elements from the first stream and then emitting
 * all of the elements from the second stream.
 *
 * A `Concat` has one `first` port, one `second` port and one `out` port.
 */
object Concat {

  final case class ConcatPorts[A](first: InPort[A], second: InPort[A], out: OutPort[A]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = List(first, second)
    override val outlets: immutable.Seq[OutPort[_]] = List(out)

    override def deepCopy(): ConcatPorts[A] =
      ConcatPorts(new InPort(first.toString), new InPort(second.toString), new OutPort(out.toString))
  }

  /**
   * Create a new `Concat` with the specified attributes.
   *
   * @param attributes optional attributes
   */
  def apply[A](attributes: OperationAttributes = OperationAttributes.none)(implicit b: FlowGraphBuilder): ConcatPorts[A] = {
    val concatModdule = new ConcatModule(
      new InPort[A]("concat.first"),
      new InPort[A]("concat.second"),
      new OutPort[A]("concat.out"),
      OperationAttributes.name("Concat") and attributes)
    b.addModule(concatModdule)
    ConcatPorts(concatModdule.first, concatModdule.second, concatModdule.out)
  }
}

object FlowGraph extends FlowGraphApply {
  import akka.stream.scaladsl.Graphs._

  class FlowGraphBuilder private[stream] () {
    private var moduleInProgress: Module = EmptyModule
    private var inPortMapping = Map.empty[StreamLayout.InPort, StreamLayout.InPort]
    private var outPortMapping = Map.empty[StreamLayout.OutPort, StreamLayout.OutPort]

    private[stream] def chainEdge[A, B](from: OutPort[A], via: Flow[A, B, _]): OutPort[B] = {
      val flowCopy = via.carbonCopy()
      moduleInProgress =
        moduleInProgress
          .grow(flowCopy.module)
          .connect(resolvePort(from), flowCopy.inlet)
      flowCopy.outlet
    }

    def addEdge[A, B](from: OutPort[A], via: Flow[A, B, _], to: InPort[B]): Unit = {
      val flowCopy = via.carbonCopy()
      moduleInProgress =
        moduleInProgress
          .grow(flowCopy.module)
          .connect(resolvePort(from), flowCopy.inlet)
          .connect(flowCopy.outlet, resolvePort(to))
    }

    def addEdge[T](from: OutPort[T], to: InPort[T]): Unit = {
      moduleInProgress = moduleInProgress.connect(resolvePort(from), resolvePort(to))
    }

    def add[T, P <: Ports](merge: FlexiMerge[T, P]): P = {
      val p = merge.ports.deepCopy().asInstanceOf[P]
      val module = new FlexiMergeModule(p, merge.createMergeLogic)
      addModule(module)
      p
    }

    def add[T, P <: Ports](route: FlexiRoute[T, P]): P = {
      val p = route.ports.deepCopy().asInstanceOf[P]
      val module = new FlexiRouteModule(p, route.createRouteLogic)
      addModule(module)
      p
    }

    // Assumes that junction is a new instance, so no copying needed here
    private[stream] def addModule(module: Module): Unit = {
      moduleInProgress = moduleInProgress.grow(module)
    }

    private[stream] def importModule(module: Module): Mapping = {
      val moduleCopy = module.carbonCopy()
      addModule(moduleCopy.module)
      moduleCopy
    }

    private[stream] def remapPorts[P <: Ports, M1, M2](graph: Graph[P, _], moduleCopy: Mapping): P = {
      /*
       * This cast should not be necessary if we could express the constraint
       * that deepCopy returns the same type as its receiver has. Would’a, could’a.
       */
      val ports = graph.ports.deepCopy().asInstanceOf[P]

      val newInPortMap = ports.inlets.zip(graph.ports.inlets) map {
        case (newGraphPort, oldGraphPort) ⇒
          newGraphPort -> moduleCopy.inPorts(oldGraphPort)
      }
      val newOutPortMap = ports.outlets.zip(graph.ports.outlets) map {
        case (newGraphPort, oldGraphPort) ⇒
          newGraphPort -> moduleCopy.outPorts(oldGraphPort)
      }
      inPortMapping ++= newInPortMap
      outPortMapping ++= newOutPortMap
      ports
    }

    /**
     * Import a graph into this module, performing a deep copy, discarding its
     * materialized value and returning the copied Ports that are now to be
     * connected.
     */
    def importGraph[P <: Ports](graph: Graph[P, _]): P = importGraph(graph, Keep.left)

    private[stream] def importGraph[P <: Ports, M1, M2](graph: Graph[P, _], combine: (M1, M2) ⇒ Any): P = {
      val moduleCopy = graph.module.carbonCopy()
      moduleInProgress = moduleInProgress.grow(
        moduleCopy.module,
        combine.asInstanceOf[(Any, Any) ⇒ Any])

      remapPorts(graph, moduleCopy)
    }

    private[stream] def resolvePort[T](port: StreamLayout.InPort): StreamLayout.InPort = {
      inPortMapping.getOrElse(port, port)
    }

    private[stream] def resolvePort[T](port: StreamLayout.OutPort): StreamLayout.OutPort = {
      outPortMapping.getOrElse(port, port)
    }

    private[stream] def andThen(port: StreamLayout.OutPort, op: StageModule): Unit = {
      addModule(op)
      moduleInProgress = moduleInProgress.connect(resolvePort(port), op.inPort)
    }

    private[stream] def buildRunnable[Mat](): RunnableFlow[Mat] = {
      if (!moduleInProgress.isRunnable) {
        throw new IllegalStateException(
          "Cannot build the RunnableFlow because there are unconnected ports: " +
            (moduleInProgress.outPorts ++ moduleInProgress.inPorts).mkString(", "))
      }
      new RunnableFlow(moduleInProgress)
    }

    private[stream] def buildSource[T, Mat](outport: OutPort[T]): Source[T, Mat] = {
      if (moduleInProgress.isRunnable)
        throw new IllegalStateException("Cannot build the Source since no ports remain open")
      if (!moduleInProgress.isSource)
        throw new IllegalStateException(
          s"Cannot build Source with open inputs (${moduleInProgress.inPorts.mkString(",")}) and outputs (${moduleInProgress.outPorts.mkString(",")})")
      if (moduleInProgress.outPorts.head != resolvePort(outport))
        throw new IllegalStateException(s"provided OutPort $outport does not equal the module’s open OutPort ${moduleInProgress.outPorts.head}")
      new Source(moduleInProgress, resolvePort(outport).asInstanceOf[OutPort[T]])
    }

    private[stream] def buildFlow[In, Out, Mat](inlet: InPort[In], outlet: OutPort[Out]): Flow[In, Out, Mat] = {
      if (!moduleInProgress.isFlow)
        throw new IllegalStateException(
          s"Cannot build Flow with open inputs (${moduleInProgress.inPorts.mkString(",")}) and outputs (${moduleInProgress.outPorts.mkString(",")})")
      if (moduleInProgress.outPorts.head != resolvePort(outlet))
        throw new IllegalStateException(s"provided OutPort $outlet does not equal the module’s open OutPort ${moduleInProgress.outPorts.head}")
      if (moduleInProgress.inPorts.head != resolvePort(inlet))
        throw new IllegalStateException(s"provided InPort $inlet does not equal the module’s open InPort ${moduleInProgress.inPorts.head}")
      new Flow(moduleInProgress, resolvePort(inlet).asInstanceOf[InPort[In]], resolvePort(outlet).asInstanceOf[OutPort[Out]])
    }

    private[stream] def buildSink[T, Mat](inport: InPort[T]): Sink[T, Mat] = {
      if (moduleInProgress.isRunnable)
        throw new IllegalStateException("Cannot build the Sink since no ports remain open")
      if (!moduleInProgress.isSink)
        throw new IllegalStateException(
          s"Cannot build Sink with open inputs (${moduleInProgress.inPorts.mkString(",")}) and outputs (${moduleInProgress.outPorts.mkString(",")})")
      if (moduleInProgress.inPorts.head != resolvePort(inport))
        throw new IllegalStateException(s"provided InPort $inport does not equal the module’s open InPort ${moduleInProgress.inPorts.head}")
      new Sink(moduleInProgress, resolvePort(inport).asInstanceOf[InPort[T]])
    }

    private[stream] def module: Module = moduleInProgress

  }

  object Implicits {

    trait CombinerBase[+T] extends Any {
      def importAndGetPort(b: FlowGraphBuilder): OutPort[T]

      def ~>(to: InPort[T])(implicit b: FlowGraphBuilder): Unit = {
        b.addEdge(importAndGetPort(b), to)
      }

      def ~>[Out](via: Flow[T, Out, _])(implicit b: FlowGraphBuilder): PortOps[Out, Unit] = {
        b.chainEdge(importAndGetPort(b), via)
      }

      def ~>(to: Sink[T, _])(implicit b: FlowGraphBuilder): Unit = {
        val sinkCopy = to.carbonCopy()
        b.addModule(sinkCopy.module)
        b.addEdge(importAndGetPort(b), sinkCopy.inlet)
      }
    }

    class PortOps[+Out, +Mat](port: StreamLayout.OutPort, b: FlowGraphBuilder) extends FlowOps[Out, Mat] with CombinerBase[Out] {
      override type Repr[+O, +M] = PortOps[O, M]

      def outlet: OutPort[Out] = port.asInstanceOf[OutPort[Out]]

      override private[scaladsl] def withAttributes(attr: OperationAttributes): Repr[Out, Mat] =
        throw new UnsupportedOperationException("Cannot set attributes on chained ops from a junction output port")

      override private[scaladsl] def andThen[U](op: StageModule): Repr[U, Mat] = {
        b.andThen(port, op)
        new PortOps(op.outPort, b)
      }

      override private[scaladsl] def andThenMat[U, Mat2](op: MaterializingStageFactory): Repr[U, Mat2] = {
        // We don't track materialization here
        b.andThen(port, op)
        new PortOps(op.outPort, b)
      }

      override def importAndGetPort(b: FlowGraphBuilder): OutPort[Out] = port.asInstanceOf[Graphs.OutPort[Out]]
    }

    import scala.language.implicitConversions
    implicit def port2flow[T](from: OutPort[T])(implicit b: FlowGraphBuilder): PortOps[T, Unit] = new PortOps(from, b)

    implicit class SourceArrow[T](val s: Source[T, _]) extends AnyVal with CombinerBase[T] {
      override def importAndGetPort(b: FlowGraphBuilder): OutPort[T] = {
        val mapping = b.importModule(s.module)
        mapping.outPorts(s.outlet).asInstanceOf[OutPort[T]]
      }
    }

  }

}
