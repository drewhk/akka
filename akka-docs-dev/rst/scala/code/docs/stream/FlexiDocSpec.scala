/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package docs.stream

import akka.stream.FlowMaterializer
import akka.stream.scaladsl._
import akka.stream.testkit.AkkaSpec
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class FlexiDocSpec extends AkkaSpec {

  implicit val ec = system.dispatcher
  implicit val mat = FlowMaterializer()

  "implement zip using readall" in {
    //#fleximerge-zip-readall
    class ZipPorts[A, B] extends FanInPorts[(A, B)] {
      val left = port[A]("left")
      val right = port[B]("right")
      override def deepCopy() = new ZipPorts
    }
    class Zip[A, B] extends FlexiMerge[(A, B), ZipPorts[A, B]](
      new ZipPorts, OperationAttributes.name("Zip1State")) {
      import FlexiMerge._
      override def createMergeLogic(p: PortT) = new MergeLogic[(A, B)] {
        override def initialState =
          State(ReadAll(p.left, p.right)) { (ctx, _, inputs) =>
            val a = inputs(p.left)
            val b = inputs(p.right)
            ctx.emit((a, b))
            SameState
          }
      }
    }
    //#fleximerge-zip-readall

    //format: OFF
    val res =
    //#fleximerge-zip-connecting
    FlowGraph(Sink.head[(Int, String)]) { implicit b =>
      o =>
      import FlowGraph.Implicits._

      val zip = b.add(new Zip[Int, String])

      Source.single(1)   ~> zip.left
      Source.single("1") ~> zip.right
                            zip.out ~> o.inlet
    }
    //#fleximerge-zip-connecting
    .run()
    //format: ON

    Await.result(res, 300.millis) should equal((1, "1"))
  }

  "implement zip using two states" in {
    //#fleximerge-zip-states
    class ZipPorts[A, B] extends FanInPorts[(A, B)] {
      val left = port[A]("left")
      val right = port[B]("right")
      override def deepCopy() = new ZipPorts
    }
    class Zip[A, B] extends FlexiMerge[(A, B), ZipPorts[A, B]](
      new ZipPorts, OperationAttributes.name("Zip2State")) {
      import FlexiMerge._

      override def createMergeLogic(p: PortT) = new MergeLogic[(A, B)] {
        var lastInA: A = _

        val readA: State[A] = State[A](Read(p.left)) { (ctx, input, element) =>
          lastInA = element
          readB
        }

        val readB: State[B] = State[B](Read(p.right)) { (ctx, input, element) =>
          ctx.emit((lastInA, element))
          readA
        }

        override def initialState: State[_] = readA
      }
    }
    //#fleximerge-zip-states

    val res = FlowGraph(Sink.head[(Int, String)]) { implicit b =>
      o =>
        import FlowGraph.Implicits._

        val zip = b.add(new Zip[Int, String])

        Source(1 to 2) ~> zip.left
        Source((1 to 2).map(_.toString)) ~> zip.right
        zip.out ~> o.inlet
    }.run()

    Await.result(res, 300.millis) should equal((1, "1"))
  }

  "fleximerge completion handling" in {
    //#fleximerge-completion
    class ImportantWithBackupPorts[A] extends FanInPorts[A] {
      val important = port[A]("important")
      val replica1 = port[A]("replica1")
      val replica2 = port[A]("replica2")
      override def deepCopy() = new ImportantWithBackupPorts
    }
    class ImportantWithBackups[A] extends FlexiMerge[A, ImportantWithBackupPorts[A]](
      new ImportantWithBackupPorts, OperationAttributes.name("ImportantWithBackups")) {
      import FlexiMerge._

      override def createMergeLogic(p: PortT) = new MergeLogic[A] {
        override def initialCompletionHandling =
          CompletionHandling(
            onComplete = (ctx, input) => input match {
              case port if port eq p.important =>
                log.info("Important input completed, shutting down.")
                ctx.complete()
                SameState

              case replica =>
                log.info("Replica {} completed, " +
                  "no more replicas available, " +
                  "applying eagerClose completion handling.", replica)

                ctx.changeCompletionHandling(eagerClose)
                SameState
            },
            onError = (ctx, input, cause) => input match {
              case port if port eq p.important =>
                ctx.error(cause)
                SameState

              case replica =>
                log.error(cause, "Replica {} failed, " +
                  "no more replicas available, " +
                  "applying eagerClose completion handling.", replica)

                ctx.changeCompletionHandling(eagerClose)
                SameState
            })

        override def initialState =
          State[A](ReadAny(p.important, p.replica1, p.replica2)) {
            (ctx, input, element) =>
              ctx.emit(element)
              SameState
          }
      }
    }
    //#fleximerge-completion

    FlowGraph() { implicit b =>
      import FlowGraph.Implicits._
      val importantWithBackups = b.add(new ImportantWithBackups[Int])
      Source.single(1) ~> importantWithBackups.important
      Source.single(2) ~> importantWithBackups.replica1
      Source.failed[Int](new Exception("Boom!") with NoStackTrace) ~> importantWithBackups.replica2
      importantWithBackups.out ~> Sink.ignore
    }.run()
  }

  "flexi preferring merge" in {
    //#flexi-preferring-merge
    class PreferringMergePorts[A] extends FanInPorts[A] {
      val preferred = port[A]("preferred")
      val secondary1 = port[A]("secondary1")
      val secondary2 = port[A]("secondary2")
      override def deepCopy() = new PreferringMergePorts
    }
    class PreferringMerge extends FlexiMerge[Int, PreferringMergePorts[Int]](
      new PreferringMergePorts, OperationAttributes.name("ImportantWithBackups")) {
      import akka.stream.scaladsl.FlexiMerge._

      override def createMergeLogic(p: PortT) = new MergeLogic[Int] {
        override def initialState =
          State[Int](ReadPreferred(p.preferred, p.secondary1, p.secondary2)) {
            (ctx, input, element) =>
              ctx.emit(element)
              SameState
          }
      }
    }
    //#flexi-preferring-merge
  }

  "flexi route" in {
    //#flexiroute-unzip
    class UnzipPorts[A, B] extends FanOutPorts[(A, B)] {
      val outA = port[A]("outA")
      val outB = port[B]("outB")
      override def deepCopy() = new UnzipPorts
    }
    class Unzip[A, B] extends FlexiRoute[(A, B), UnzipPorts[A, B]](
      new UnzipPorts, OperationAttributes.name("Unzip")) {
      import FlexiRoute._

      override def createRouteLogic(p: PortT) = new RouteLogic[(A, B)] {
        override def initialState =
          State[Any](DemandFromAll(p.outA, p.outB)) {
            (ctx, _, element) =>
              val (a, b) = element
              ctx.emit(p.outA, a)
              ctx.emit(p.outB, b)
              SameState
          }

        override def initialCompletionHandling = eagerClose
      }
    }
    //#flexiroute-unzip
  }

  "flexi route completion handling" in {
    //#flexiroute-completion
    class ImportantRoutePorts[A] extends FanOutPorts[A] {
      val important = port[A]("important")
      val additional1 = port[A]("additional1")
      val additional2 = port[A]("additional2")
      override def deepCopy() = new ImportantRoutePorts
    }
    class ImportantRoute[A] extends FlexiRoute[A, ImportantRoutePorts[A]](
      new ImportantRoutePorts, OperationAttributes.name("ImportantRoute")) {
      import FlexiRoute._
      override def createRouteLogic(p: PortT) = new RouteLogic[A] {
        override def initialCompletionHandling =
          CompletionHandling(
            // upstream:
            onComplete = (ctx) => (),
            onError = (ctx, thr) => (),
            // downstream:
            onCancel = (ctx, output) => output match {
              case port if port eq p.important =>
                // complete all downstreams, and cancel the upstream
                ctx.complete()
                SameState
              case _ =>
                SameState
            })

        override def initialState =
          State[A](DemandFromAny(p.important, p.additional1, p.additional2)) {
            (ctx, output, element) =>
              ctx.emit(output, element)
              SameState
          }
      }
    }
    //#flexiroute-completion

    FlowGraph() { implicit b =>
      import FlowGraph.Implicits._
      val route = b.add(new ImportantRoute[Int])
      Source.single(1) ~> route.in
      route.important ~> Sink.ignore
      route.additional1 ~> Sink.ignore
      route.additional2 ~> Sink.ignore
    }.run()
  }

  "flexi route completion handling emitting element upstream completion" in {
    class ElementsAndStatusPorts[A] extends FanOutPorts[A] {
      val out = port[A]("out")
      override def deepCopy() = new ElementsAndStatusPorts
    }
    class ElementsAndStatus[A] extends FlexiRoute[A, ElementsAndStatusPorts[A]](
      new ElementsAndStatusPorts, OperationAttributes.none) {
      import FlexiRoute._

      override def createRouteLogic(p: PortT) = new RouteLogic[A] {
        // format: OFF
        //#flexiroute-completion-upstream-completed-signalling
        var buffer: List[A]
        //#flexiroute-completion-upstream-completed-signalling
          = List[A]()
        // format: ON

        //#flexiroute-completion-upstream-completed-signalling

        def drainBuffer(ctx: RouteLogicContext[Any]): Unit =
          while (ctx.isDemandAvailable(p.out) && buffer.nonEmpty) {
            ctx.emit(p.out, buffer.head)
            buffer = buffer.tail
          }

        val signalStatusOnTermination = CompletionHandling(
          onComplete = ctx => drainBuffer(ctx),
          onError = (ctx, cause) => drainBuffer(ctx),
          onCancel = (_, _) => SameState)
        //#flexiroute-completion-upstream-completed-signalling

        override def initialCompletionHandling = signalStatusOnTermination

        override def initialState = State[A](DemandFromAny(p.out)) {
          (ctx, output, element) =>
            ctx.emit(output, element)
            SameState
        }
      }
    }
  }
}
