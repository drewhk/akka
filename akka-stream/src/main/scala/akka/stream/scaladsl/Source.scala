/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import akka.stream.impl.Stages.{ MaterializingStageFactory, StageModule }
import akka.stream.scaladsl.Graphs.SourcePorts
import akka.stream.stage.{ TerminationDirective, Directive, Context, PushPullStage }

import scala.annotation.unchecked.uncheckedVariance
import scala.language.higherKinds
import akka.actor.Props
import akka.stream.impl.{ EmptyPublisher, ErrorPublisher, SynchronousIterablePublisher }
import org.reactivestreams.Publisher
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }
import akka.stream.FlowMaterializer
import akka.stream.impl._
import akka.actor.Cancellable
import akka.actor.ActorRef
import scala.concurrent.Promise
import org.reactivestreams.Subscriber

/**
 * A `Source` is a set of stream processing steps that has one open output and an attached input.
 * Can be used as a `Publisher`
 */
final class Source[+Out, +Mat] private (m: StreamLayout.Module, val outlet: Graphs.OutPort[Out])
  extends FlowOps[Out, Mat] with Graphs.Graph[Graphs.SourcePorts[Out], Mat] {
  private[stream] override val module: StreamLayout.Module = m

  def this(sourceModule: SourceModule[Out @uncheckedVariance, Mat]) = this(sourceModule, sourceModule.outPort)

  override type Repr[+O, +M] = Source[O, M]

  override val ports = SourcePorts[Out](outlet)

  def via[T, Mat2](flow: Flow[Out, T, Mat2]): Source[T, Mat2] =
    via(flow, (sourcem: Mat, flowm: Mat2) ⇒ flowm)

  /**
   * Transform this [[akka.stream.scaladsl.Source]] by appending the given processing stages.
   */
  def via[T, Mat2, Mat3](flow: Flow[Out, T, Mat2], combine: (Mat, Mat2) ⇒ Mat3): Source[T, Mat3] = {
    val flowCopy = flow.carbonCopy()
    new Source(
      module.grow(flowCopy.module, combine)
        .connect(outlet, flowCopy.ports.inlet), flow.ports.outlet)
  }

  def to[Mat2](sink: Sink[Out, Mat2]): RunnableFlow[Mat2] =
    to(sink, (sourcem: Mat, sinkm: Mat2) ⇒ sinkm)

  /**
   * Connect this [[akka.stream.scaladsl.Source]] to a [[akka.stream.scaladsl.Sink]],
   * concatenating the processing steps of both.
   */
  def to[Mat2, Mat3](sink: Sink[Out, Mat2], combine: (Mat, Mat2) ⇒ Mat3): RunnableFlow[Mat3] = {
    val sinkCopy = sink.module.carbonCopy()
    RunnableFlow(module
      .grow(sinkCopy.module, combine)
      .connect(outlet, sinkCopy.inPorts(sink.backwardPort)))
  }

  /** INTERNAL API */
  override private[scaladsl] def andThen[U](op: StageModule): Repr[U, Mat] = {
    // No need to copy here, op is a fresh instance
    // FIXME: currently combine ignores here
    new Source(module.grow(op).connect(outlet, op.inPort), op.outPort.asInstanceOf[Graphs.OutPort[U]])
  }

  override private[scaladsl] def andThenMat[U, Mat2](op: MaterializingStageFactory): Repr[U, Mat2] = {
    new Source(module.grow(op, (m: Mat, m2: Mat2) ⇒ m2).connect(outlet, op.inPort), op.outPort.asInstanceOf[Graphs.OutPort[U]])
  }

  /**
   * Connect this `Source` to a `Sink` and run it. The returned value is the materialized value
   * of the `Sink`, e.g. the `Publisher` of a [[akka.stream.scaladsl.Sink#publisher]].
   */
  def runWith[Mat2](sink: Sink[Out, Mat2])(implicit materializer: FlowMaterializer): Mat2 = to(sink).run()

  /**
   * Shortcut for running this `Source` with a fold function.
   * The given function is invoked for every received element, giving it its previous
   * output (or the given `zero` value) and the element as input.
   * The returned [[scala.concurrent.Future]] will be completed with value of the final
   * function evaluation when the input stream ends, or completed with `Failure`
   * if there is an error is signaled in the stream.
   */
  def runFold[U](zero: U)(f: (U, Out) ⇒ U)(implicit materializer: FlowMaterializer): Future[U] =
    runWith(Sink.fold(zero)(f)) // FIXME why is fold always an end step?

  /**
   * Shortcut for running this `Source` with a foreach procedure. The given procedure is invoked
   * for each received element.
   * The returned [[scala.concurrent.Future]] will be completed with `Success` when reaching the
   * normal end of the stream, or completed with `Failure` if there is an error is signaled in
   * the stream.
   */
  def runForeach(f: Out ⇒ Unit)(implicit materializer: FlowMaterializer): Future[Unit] = runWith(Sink.foreach(f))

  /**
   * Concatenates a second source so that the first element
   * emitted by that source is emitted after the last element of this
   * source.
   */
  def concat[Out2 >: Out, Mat2](second: Source[Out2, Mat2]): Source[Out2, (Mat, Mat2)] = Source.concat(this, second)

  /**
   * Concatenates a second source so that the first element
   * emitted by that source is emitted after the last element of this
   * source.
   *
   * This is a shorthand for [[concat]]
   */
  def ++[Out2 >: Out, Mat2](second: Source[Out2, Mat2]): Source[Out2, (Mat, Mat2)] = concat(second)

  /**
   * Applies given [[OperationAttributes]] to a given section.
   */
  def section[O, O2 >: Out, Mat2, Mat3](attributes: OperationAttributes, combine: (Mat, Mat2) ⇒ Mat3)(section: Flow[O2, O2, Unit] ⇒ Flow[O2, O, Mat2]): Source[O, Mat3] = {
    val subFlow = section(Flow[O2]).withAttributes(attributes).carbonCopy()
    new Source(
      module
        .grow(subFlow.module.wrap(), combine)
        .connect(outlet, subFlow.ports.inlet),
      subFlow.ports.outlet)
  }

  def section[O, O2 >: Out, Mat2](attributes: OperationAttributes)(section: Flow[O2, O2, Unit] ⇒ Flow[O2, O, Mat2]): Source[O, Mat2] = {
    this.section[O, O2, Mat2, Mat2](attributes, (parentm: Mat, subm: Mat2) ⇒ subm)(section)
  }

  /** INTERNAL API */
  override private[scaladsl] def withAttributes(attr: OperationAttributes): Repr[Out, Mat] = {
    val newModule = module.withAttributes(attr)
    new Source(newModule, outlet)
  }

}

object Source {
  private[stream] def apply[Out, Mat](module: SourceModule[Out, Mat]): Source[Out, Mat] =
    new Source(module)

  /**
   * Helper to create [[Source]] from `Publisher`.
   *
   * Construct a transformation starting with given publisher. The transformation steps
   * are executed by a series of [[org.reactivestreams.Processor]] instances
   * that mediate the flow of elements downstream and the propagation of
   * back-pressure upstream.
   */
  def apply[T](publisher: Publisher[T]): Source[T, Unit] = new Source(new PublisherSource(publisher))

  /**
   * Helper to create [[Source]] from `Iterator`.
   * Example usage: `Source(() => Iterator.from(0))`
   *
   * Start a new `Source` from the given function that produces anIterator.
   * The produced stream of elements will continue until the iterator runs empty
   * or fails during evaluation of the `next()` method.
   * Elements are pulled out of the iterator in accordance with the demand coming
   * from the downstream transformation steps.
   */
  def apply[T](f: () ⇒ Iterator[T]): Source[T, Unit] = {
    apply(new immutable.Iterable[T] {
      override def iterator: Iterator[T] = f()
    })
  }

  /**
   * Helper to create [[Source]] from `Iterable`.
   * Example usage: `Source(Seq(1,2,3))`
   *
   * Starts a new `Source` from the given `Iterable`. This is like starting from an
   * Iterator, but every Subscriber directly attached to the Publisher of this
   * stream will see an individual flow of elements (always starting from the
   * beginning) regardless of when they subscribed.
   */
  def apply[T](iterable: immutable.Iterable[T]): Source[T, Unit] = {

    Source.empty.transform(() ⇒ {
      new PushPullStage[Nothing, T] {
        var iterator: Iterator[T] = null

        // Delayed init so we onError instead of failing during construction of the Source
        def initIterator(): Unit = if (iterator eq null) iterator = iterable.iterator

        // Upstream is guaranteed to be empty
        override def onPush(elem: Nothing, ctx: Context[T]): Directive =
          throw new UnsupportedOperationException("The IterableSource stage cannot be pushed")

        override def onUpstreamFinish(ctx: Context[T]): TerminationDirective = {
          initIterator()
          if (iterator.hasNext) ctx.absorbTermination()
          else ctx.finish()
        }

        override def onPull(ctx: Context[T]): Directive = {
          if (!ctx.isFinishing) {
            initIterator()
            ctx.pull()
          } else {
            val elem = iterator.next()
            if (iterator.hasNext) ctx.push(elem)
            else ctx.pushAndFinish(elem)
          }
        }
      }

    })
  }

  /**
   * Start a new `Source` from the given `Future`. The stream will consist of
   * one element when the `Future` is completed with a successful value, which
   * may happen before or after materializing the `Flow`.
   * The stream terminates with an error if the `Future` is completed with a failure.
   */
  def apply[T](future: Future[T]): Source[T, Unit] = new Source(new FutureSource(future))

  /**
   * Elements are emitted periodically with the specified interval.
   * The tick element will be delivered to downstream consumers that has requested any elements.
   * If a consumer has not requested any elements at the point in time when the tick
   * element is produced it will not receive that tick element later. It will
   * receive new tick elements as soon as it has requested more elements.
   */
  def apply[T](initialDelay: FiniteDuration, interval: FiniteDuration, tick: T): Source[T, Cancellable] =
    new Source(new TickSource(initialDelay, interval, tick))

  /**
   * Creates a `Source` by using an empty [[FlowGraphBuilder]] on a block that expects a [[FlowGraphBuilder]] and
   * returns the `UndefinedSink`.
   */
  //  def apply[T]()(block: FlowGraphBuilder ⇒ UndefinedSink[T]): Source[T] =
  //    createSourceFromBuilder(new FlowGraphBuilder(), block)

  /**
   * Creates a `Source` by using a [[FlowGraphBuilder]] from this [[PartialFlowGraph]] on a block that expects
   * a [[FlowGraphBuilder]] and returns the `UndefinedSink`.
   */
  //  def apply[T](graph: PartialFlowGraph)(block: FlowGraphBuilder ⇒ UndefinedSink[T]): Source[T] =
  //    createSourceFromBuilder(new FlowGraphBuilder(graph), block)

  //  private def createSourceFromBuilder[T](builder: FlowGraphBuilder, block: FlowGraphBuilder ⇒ UndefinedSink[T]): Source[T] = {
  //    val out = block(builder)
  //    builder.partialBuild().toSource(out)
  //  }

  /**
   * Creates a `Source` that is materialized to an [[akka.actor.ActorRef]] which points to an Actor
   * created according to the passed in [[akka.actor.Props]]. Actor created by the `props` should
   * be [[akka.stream.actor.ActorPublisher]].
   */
  def apply[T](props: Props): Source[T, ActorRef] = new Source(new PropsSource(props))

  /**
   * Create a `Source` with one element.
   * Every connected `Sink` of this stream will see an individual stream consisting of one element.
   */
  def single[T](element: T): Source[T, Unit] = apply(SynchronousIterablePublisher(List(element), "single")) // FIXME optimize

  /**
   * A `Source` with no elements, i.e. an empty stream that is completed immediately for every connected `Sink`.
   */
  def empty[T](): Source[T, Unit] = _empty
  private[this] val _empty: Source[Nothing, Unit] = apply(EmptyPublisher)

  /**
   * Create a `Source` with no elements, which does not complete its downstream,
   * until externally triggered to do so.
   *
   * It materializes a [[scala.concurrent.Promise]] which will be completed
   * when the downstream stage of this source cancels. This promise can also
   * be used to externally trigger completion, which the source then signalls
   * to its downstream.
   */
  def lazyEmpty[T](): Source[T, Promise[Unit]] = new Source(new LazyEmptySource[T]())

  /**
   * Create a `Source` that immediately ends the stream with the `cause` error to every connected `Sink`.
   */
  def failed[T](cause: Throwable): Source[T, Unit] = apply(ErrorPublisher(cause, "failed"))

  /**
   * Concatenates two sources so that the first element
   * emitted by the second source is emitted after the last element of the first
   * source.
   */
  def concat[T, Mat1, Mat2](source1: Source[T, Mat1], source2: Source[T, Mat2]): Source[T, (Mat1, Mat2)] = ??? // FIXME

  /**
   * Creates a `Source` that is materialized as a [[org.reactivestreams.Subscriber]]
   */
  def subscriber[T]: Source[T, Subscriber[T]] = new Source(new SubscriberSource[T])
}